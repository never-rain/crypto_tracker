use crate::detector::Candle;
use reqwest::StatusCode;
use reqwest::blocking::{Client, RequestBuilder, Response};
use serde::Deserialize;
use std::collections::{BTreeMap, HashMap};
use std::net::TcpStream;
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tungstenite::{Message, WebSocket, connect};

const REST_BASE_URL: &str = "https://data-api.binance.vision";
const WS_URL: &str = "wss://data-stream.binance.vision/ws";
const MAX_RETRIES: usize = 5;

#[derive(Deserialize)]
pub struct ExchangeInfo {
    pub symbols: Vec<SymbolInfo>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SymbolInfo {
    pub symbol: String,
    pub status: String,
    pub quote_asset: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct Ticker24h {
    symbol: String,
    quote_volume: String,
}

pub fn fetch_exchange_info(client: &Client) -> Result<ExchangeInfo, Box<dyn std::error::Error>> {
    let response = send_with_retry(client.get(format!("{REST_BASE_URL}/api/v3/exchangeInfo")))?;
    Ok(response.json()?)
}

pub fn fetch_quote_volumes_24h(
    client: &Client,
) -> Result<HashMap<String, f64>, Box<dyn std::error::Error>> {
    let response = send_with_retry(client.get(format!("{REST_BASE_URL}/api/v3/ticker/24hr")))?;
    let tickers: Vec<Ticker24h> = response.json()?;
    Ok(tickers
        .into_iter()
        .filter_map(|ticker| {
            ticker
                .quote_volume
                .parse::<f64>()
                .ok()
                .map(|volume| (ticker.symbol, volume))
        })
        .collect())
}

pub fn fetch_closed_candles(
    client: &Client,
    symbol: &str,
    interval: &str,
    required_candles: usize,
) -> Result<Vec<Candle>, Box<dyn std::error::Error>> {
    let now_ms = unix_time_ms();
    let mut end_time_ms = now_ms;
    let mut candles = BTreeMap::<i64, Candle>::new();

    while candles.len() < required_candles {
        let remaining = required_candles - candles.len();
        let limit = (remaining + 1).min(1000);
        let response = send_with_retry(
            client
                .get(format!("{REST_BASE_URL}/api/v3/klines"))
                .query(&[
                    ("symbol", symbol.to_string()),
                    ("interval", interval.to_string()),
                    ("limit", limit.to_string()),
                    ("endTime", end_time_ms.to_string()),
                ]),
        )?;
        let rows: Vec<Vec<serde_json::Value>> = response.json()?;
        if rows.is_empty() {
            break;
        }

        let earliest_open = rows.iter().filter_map(|row| json_i64(row.first())).min();
        for row in rows {
            if let Some(candle) = parse_kline(&row)
                && candle.close_time_ms <= now_ms
            {
                candles.insert(candle.open_time_ms, candle);
            }
        }
        let Some(earliest_open) = earliest_open else {
            break;
        };
        if earliest_open <= 0 || earliest_open >= end_time_ms {
            break;
        }
        end_time_ms = earliest_open - 1;
    }

    let mut result: Vec<Candle> = candles.into_values().collect();
    if result.len() > required_candles {
        result.drain(..result.len() - required_candles);
    }
    Ok(result)
}

fn parse_kline(row: &[serde_json::Value]) -> Option<Candle> {
    Some(Candle {
        open_time_ms: json_i64(row.first())?,
        open: json_f64(row.get(1))?,
        high: json_f64(row.get(2))?,
        low: json_f64(row.get(3))?,
        close: json_f64(row.get(4))?,
        close_time_ms: json_i64(row.get(6))?,
        quote_volume: json_f64(row.get(7))?,
    })
}

fn json_i64(value: Option<&serde_json::Value>) -> Option<i64> {
    value?.as_i64()
}

fn json_f64(value: Option<&serde_json::Value>) -> Option<f64> {
    match value? {
        serde_json::Value::String(value) => value.parse().ok(),
        serde_json::Value::Number(value) => value.as_f64(),
        _ => None,
    }
}

fn send_with_retry(request: RequestBuilder) -> Result<Response, Box<dyn std::error::Error>> {
    let mut backoff_secs = 1_u64;
    for attempt in 0..=MAX_RETRIES {
        let retryable_request = request
            .try_clone()
            .ok_or("could not clone Binance request for retry")?;
        match retryable_request.send() {
            Ok(response) if response.status().is_success() => return Ok(response),
            Ok(response)
                if response.status() == StatusCode::TOO_MANY_REQUESTS
                    || response.status() == StatusCode::IM_A_TEAPOT =>
            {
                if attempt == MAX_RETRIES {
                    return Err(format!(
                        "Binance rate limit persisted after {MAX_RETRIES} retries"
                    )
                    .into());
                }
                let retry_after = response
                    .headers()
                    .get(reqwest::header::RETRY_AFTER)
                    .and_then(|value| value.to_str().ok())
                    .and_then(|value| value.parse::<u64>().ok())
                    .unwrap_or(backoff_secs)
                    .max(1);
                thread::sleep(Duration::from_secs(retry_after));
            }
            Ok(response) if response.status().is_server_error() => {
                if attempt == MAX_RETRIES {
                    return Err(
                        format!("Binance server error persisted: {}", response.status()).into(),
                    );
                }
                thread::sleep(Duration::from_secs(backoff_secs));
            }
            Ok(response) => {
                let status = response.status();
                let body = response.text().unwrap_or_default();
                return Err(format!("Binance request failed: {status} {body}").into());
            }
            Err(error) => {
                if attempt == MAX_RETRIES {
                    return Err(error.into());
                }
                thread::sleep(Duration::from_secs(backoff_secs));
            }
        }
        backoff_secs = (backoff_secs * 2).min(60);
    }
    unreachable!()
}

fn unix_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[derive(Deserialize)]
pub struct WsKlineEvent {
    #[serde(rename = "s")]
    pub symbol: String,
    #[serde(rename = "k")]
    pub kline: WsKline,
}

#[derive(Deserialize)]
pub struct WsKline {
    #[serde(rename = "t")]
    pub open_time_ms: i64,
    #[serde(rename = "T")]
    pub close_time_ms: i64,
    #[serde(rename = "x")]
    pub is_closed: bool,
    #[serde(rename = "o")]
    pub open: String,
    #[serde(rename = "h")]
    pub high: String,
    #[serde(rename = "l")]
    pub low: String,
    #[serde(rename = "c")]
    pub close: String,
    #[serde(rename = "q")]
    pub quote_volume: String,
}

impl WsKline {
    pub fn into_candle(self) -> Option<Candle> {
        Some(Candle {
            open_time_ms: self.open_time_ms,
            close_time_ms: self.close_time_ms,
            open: self.open.parse().ok()?,
            high: self.high.parse().ok()?,
            low: self.low.parse().ok()?,
            close: self.close.parse().ok()?,
            quote_volume: self.quote_volume.parse().ok()?,
        })
    }
}

#[derive(Deserialize)]
pub struct WsSubscriptionAck {
    pub id: Option<u64>,
}

pub type KlineSocket = WebSocket<tungstenite::stream::MaybeTlsStream<TcpStream>>;

pub fn connect_kline_stream(
    symbols: &[String],
    interval: &str,
) -> Result<KlineSocket, Box<dyn std::error::Error>> {
    let (mut socket, _) = connect(WS_URL)?;
    let params: Vec<String> = symbols
        .iter()
        .map(|symbol| format!("{}@kline_{interval}", symbol.to_lowercase()))
        .collect();
    socket.send(Message::Text(
        serde_json::json!({"method": "SUBSCRIBE", "params": params, "id": 1}).to_string(),
    ))?;
    Ok(socket)
}

pub fn set_socket_read_timeout(
    socket: &mut KlineSocket,
    timeout: Duration,
) -> Result<(), Box<dyn std::error::Error>> {
    match socket.get_mut() {
        tungstenite::stream::MaybeTlsStream::Plain(stream) => {
            stream.set_read_timeout(Some(timeout))?
        }
        tungstenite::stream::MaybeTlsStream::NativeTls(stream) => {
            stream.get_mut().set_read_timeout(Some(timeout))?;
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_complete_rest_kline() {
        let row = serde_json::json!([1, "100", "106", "99", "101", "10", 3_600_000, "5000"]);
        let candle = parse_kline(row.as_array().unwrap()).unwrap();
        assert_eq!(candle.open_time_ms, 1);
        assert_eq!(candle.high, 106.0);
        assert_eq!(candle.quote_volume, 5000.0);
    }

    #[test]
    fn rejects_malformed_rest_kline() {
        let row = serde_json::json!([1, "invalid"]);
        assert!(parse_kline(row.as_array().unwrap()).is_none());
    }
}
