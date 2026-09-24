use crate::config::Config;
use crate::detector::CandidateResult;
use chrono::{DateTime, Local};
use reqwest::blocking::Client;

const TELEGRAM_SAFE_MESSAGE_LEN: usize = 3_800;

pub fn send_candidate_alert(
    client: &Client,
    config: &Config,
    candidate: &CandidateResult,
) -> Result<(), Box<dyn std::error::Error>> {
    send_telegram_message(
        client,
        &config.telegram_token,
        &config.telegram_chat_id,
        &candidate_message(config, candidate),
    )
}

pub fn send_candidate_summaries(
    client: &Client,
    config: &Config,
    candidates: &[CandidateResult],
) -> Result<(), Box<dyn std::error::Error>> {
    if candidates.is_empty() {
        return Ok(());
    }
    let heading = escape_markdown_v2(&format!(
        "Historische Kandidaten: {}\nFenster: {}, Kerzen: {}\n\n",
        candidates.len(),
        config.rolling_window,
        config.candle_interval
    ));
    let mut message = heading.clone();
    for candidate in candidates {
        let metrics = escape_markdown_v2(&format!(
            " | im Band {:.1}% | letzter Spike +{:.2}% | größter Spike +{:.2}% | Volumen {:.0}\n",
            candidate.sideways_close_ratio,
            candidate.latest_spike_pct,
            candidate.largest_spike_pct,
            candidate.quote_volume_24h
        ));
        let line = format!("{}{metrics}", trade_link(&candidate.symbol));
        if message.len() + line.len() > TELEGRAM_SAFE_MESSAGE_LEN {
            send_telegram_message(
                client,
                &config.telegram_token,
                &config.telegram_chat_id,
                &message,
            )?;
            message = heading.clone();
        }
        message.push_str(&line);
    }
    if message != heading {
        send_telegram_message(
            client,
            &config.telegram_token,
            &config.telegram_chat_id,
            &message,
        )?;
    }
    Ok(())
}

pub fn candidate_message(config: &Config, candidate: &CandidateResult) -> String {
    let quiet_hours = candidate.quiet_period_ms as f64 / 3_600_000.0;
    let text = format!(
        "Seitwärts-Kandidat\n\nAktueller Close: {:.8}\nMedian: {:.8}\nBand: {:.8} bis {:.8}\nCloses im Band: {:.2}%\n\nJüngster Spike: +{:.2}% am {}\nJüngstes Spike-High: {:.8}\nGrößter Spike: +{:.2}% am {}\nRuhe seit Spike: {:.1}h\n\nFenster: {}\nKerzen: {}\n24h Quote-Volumen: {:.0}",
        candidate.latest_close,
        candidate.median_price,
        candidate.band_lower,
        candidate.band_upper,
        candidate.sideways_close_ratio,
        candidate.latest_spike_pct,
        format_epoch_ms(candidate.latest_spike_time_ms),
        candidate.latest_spike_high,
        candidate.largest_spike_pct,
        format_epoch_ms(candidate.largest_spike_time_ms),
        quiet_hours,
        config.rolling_window,
        config.candle_interval,
        candidate.quote_volume_24h
    );
    format!(
        "{}\n\n{}",
        escape_markdown_v2(&text),
        trade_link(&candidate.symbol)
    )
}

pub fn send_telegram_message(
    client: &Client,
    token: &str,
    chat_id: &str,
    text: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let url = format!("https://api.telegram.org/bot{token}/sendMessage");
    let response = client
        .post(url)
        .json(&serde_json::json!({
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "MarkdownV2",
            "disable_web_page_preview": true
        }))
        .send()?;
    if !response.status().is_success() {
        let status = response.status();
        let body = response.text().unwrap_or_default();
        return Err(format!("telegram sendMessage failed: {status} {body}").into());
    }
    Ok(())
}

fn display_pair(symbol: &str) -> String {
    symbol
        .strip_suffix("USDT")
        .map_or_else(|| symbol.to_string(), |base| format!("{base}/USDT"))
}

fn trade_link(symbol: &str) -> String {
    let base = symbol.strip_suffix("USDT").unwrap_or(symbol);
    let url = format!("https://www.binance.com/de/trade/{base}_USDT");
    format!("[{}]({url})", escape_markdown_v2(&display_pair(symbol)))
}

fn format_epoch_ms(timestamp_ms: i64) -> String {
    DateTime::from_timestamp_millis(timestamp_ms)
        .map(|time| {
            time.with_timezone(&Local)
                .format("%d.%m.%Y %H:%M")
                .to_string()
        })
        .unwrap_or_else(|| timestamp_ms.to_string())
}

pub fn escape_markdown_v2(text: &str) -> String {
    let mut escaped = String::with_capacity(text.len());
    for ch in text.chars() {
        match ch {
            '_' | '*' | '[' | ']' | '(' | ')' | '~' | '`' | '>' | '#' | '+' | '-' | '=' | '|'
            | '{' | '}' | '.' | '!' => {
                escaped.push('\\');
                escaped.push(ch);
            }
            _ => escaped.push(ch),
        }
    }
    escaped
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escapes_markdown_control_characters() {
        assert_eq!(escape_markdown_v2("A+B.C"), "A\\+B\\.C");
    }

    #[test]
    fn candidate_message_contains_strategy_metrics() {
        let candidate = CandidateResult {
            symbol: "BTCUSDT".into(),
            median_price: 100.0,
            band_lower: 98.0,
            band_upper: 102.0,
            sideways_close_ratio: 96.0,
            latest_close: 100.0,
            latest_spike_time_ms: 0,
            latest_spike_high: 106.0,
            latest_spike_pct: 6.0,
            largest_spike_time_ms: 0,
            largest_spike_pct: 8.0,
            quiet_period_ms: 3_600_000,
            quote_volume_24h: 200_000_000.0,
        };
        let message = candidate_message(&Config::default(), &candidate);
        assert!(message.contains("[BTC/USDT](https://www.binance.com/de/trade/BTC_USDT)"));
        assert!(message.contains("Closes im Band"));
        assert!(message.contains("96\\.00%"));
    }

    #[test]
    fn trade_link_uses_requested_binance_pair_url() {
        assert_eq!(
            trade_link("BICOUSDT"),
            "[BICO/USDT](https://www.binance.com/de/trade/BICO_USDT)"
        );
    }
}
