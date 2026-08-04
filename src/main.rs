mod binance;
mod config;
mod telegram;
mod time_utils;

use binance::{
    SymbolInfo, WsKlineEvent, WsSubscriptionAck, connect_kline_stream, fetch_exchange_info,
    fetch_klines, fetch_quote_volumes_24h, set_socket_read_timeout,
};
use chrono::Local;
use config::{Config, parse_config, save_config_file, try_reload_config};
use std::collections::{HashMap, HashSet, VecDeque};
use std::io::ErrorKind;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use telegram::{escape_markdown_v2, send_telegram_message};
use time_utils::{format_timestamp_de, format_timestamp_short};
use tungstenite::Message;

const WS_BATCH_SIZE: usize = 100;

struct SymbolUniverse {
    trading_usdt_symbols: Vec<String>,
    tracked_symbols: Vec<String>,
}

struct FreshListingState {
    detected_at: Instant,
    last_candle_count: usize,
    momentum_alert_active: bool,
}

impl FreshListingState {
    fn new() -> Self {
        Self {
            detected_at: Instant::now(),
            last_candle_count: 0,
            momentum_alert_active: false,
        }
    }
}

fn candle_interval_secs(interval: &str) -> Result<u64, Box<dyn std::error::Error>> {
    if interval.len() < 2 {
        return Err(format!("invalid candle interval: {}", interval).into());
    }

    let (value, unit) = interval.split_at(interval.len() - 1);
    let value: u64 = value.parse()?;
    let seconds = match unit {
        "s" => value,
        "m" => value * 60,
        "h" => value * 60 * 60,
        "d" => value * 60 * 60 * 24,
        "w" => value * 60 * 60 * 24 * 7,
        _ => return Err(format!("unsupported candle interval: {}", interval).into()),
    };
    Ok(seconds)
}

fn log_config(config: &Config, source: &str) {
    println!(
        "{}: Loaded config from {} -> min_quote_volume_24h={}, interval_secs={}, lookback_candles={}, change_threshold_pct={}, bearish_change_threshold_pct={}, candle_interval={}, streak_len={}, listing_poll_secs={}, fresh_listing_candle_interval={}, fresh_listing_ttl_mins={}",
        format_timestamp_short(Local::now()),
        source,
        config.min_quote_volume_24h,
        config.interval_secs,
        config.lookback_candles,
        config.change_threshold_pct,
        config.bearish_change_threshold_pct,
        config.candle_interval,
        config.streak_len,
        config.listing_poll_secs,
        config.fresh_listing_candle_interval,
        config.fresh_listing_ttl_mins
    );
}

fn fetch_trading_usdt_symbols(
    client: &reqwest::blocking::Client,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    let info = fetch_exchange_info(client)?;
    let mut tradable: Vec<&SymbolInfo> = info
        .symbols
        .iter()
        .filter(|symbol| symbol.status == "TRADING" && symbol.quote_asset == "USDT")
        .collect();

    tradable.sort_by(|a, b| a.symbol.cmp(&b.symbol));
    let symbols: Vec<String> = tradable.into_iter().map(|s| s.symbol.clone()).collect();
    println!(
        "{}: Fetched {} TRADING USDT symbols from exchangeInfo",
        format_timestamp_short(Local::now()),
        symbols.len()
    );
    Ok(symbols)
}

fn load_symbol_universe(
    client: &reqwest::blocking::Client,
    min_quote_volume_24h: f64,
) -> Result<SymbolUniverse, Box<dyn std::error::Error>> {
    let trading_usdt_symbols = fetch_trading_usdt_symbols(client)?;
    let volumes = fetch_quote_volumes_24h(client)?;
    let mut tracked_symbols: Vec<String> = trading_usdt_symbols
        .iter()
        .filter(|symbol| {
            volumes
                .get(symbol.as_str())
                .is_some_and(|volume| *volume >= min_quote_volume_24h)
        })
        .cloned()
        .collect();
    tracked_symbols.sort();

    Ok(SymbolUniverse {
        trading_usdt_symbols,
        tracked_symbols,
    })
}

fn extract_closed_closes(
    klines: Vec<Vec<serde_json::Value>>,
    max_closed_klines: usize,
) -> VecDeque<f64> {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or(Duration::from_secs(0))
        .as_millis() as i64;
    let mut closes = VecDeque::with_capacity(max_closed_klines);

    for kline in klines {
        let close_time_ms = match kline.get(6) {
            Some(serde_json::Value::Number(value)) => value.as_i64(),
            _ => None,
        };
        let Some(close_time_ms) = close_time_ms else {
            continue;
        };
        if close_time_ms > now_ms {
            continue;
        }

        let close = match kline.get(4).and_then(|value| value.as_str()) {
            Some(value) => value.parse::<f64>().ok(),
            None => None,
        };
        let Some(close) = close else {
            continue;
        };

        if closes.len() == max_closed_klines {
            closes.pop_front();
        }
        closes.push_back(close);
    }

    closes
}

fn seed_closes(
    client: &reqwest::blocking::Client,
    symbols: &[String],
    candle_interval: &str,
    required_closed_klines: usize,
) -> Result<HashMap<String, VecDeque<f64>>, Box<dyn std::error::Error>> {
    let mut state = HashMap::with_capacity(symbols.len());

    for symbol in symbols {
        let limit = required_closed_klines + 1;
        let klines = fetch_klines(client, symbol, candle_interval, limit)?;
        let closes = extract_closed_closes(klines, required_closed_klines);
        println!(
            "{}: Seeded {} closed klines for {}",
            format_timestamp_short(Local::now()),
            closes.len(),
            symbol
        );
        state.insert(symbol.clone(), closes);
    }

    Ok(state)
}

fn find_matching_window(
    closes: &VecDeque<f64>,
    streak_len: usize,
    threshold: f64,
) -> Option<Vec<String>> {
    let values: Vec<f64> = closes.iter().copied().collect();
    for change_window in values.windows(streak_len + 1) {
        let mut change_parts: Vec<String> = Vec::with_capacity(streak_len);
        let mut window_ok = true;

        for pair in change_window.windows(2) {
            let prev = pair[0];
            let last = pair[1];
            if prev <= 0.0 {
                window_ok = false;
                break;
            }
            let change_pct = (last - prev) / prev * 100.0;
            if change_pct < threshold {
                window_ok = false;
                break;
            }
            change_parts.push(format!("{:.2}%", change_pct));
        }

        if window_ok {
            return Some(change_parts);
        }
    }

    None
}

fn find_bearish_matching_window(
    closes: &VecDeque<f64>,
    streak_len: usize,
    threshold: f64,
) -> Option<Vec<String>> {
    let values: Vec<f64> = closes.iter().copied().collect();
    for change_window in values.windows(streak_len + 1) {
        let mut change_parts: Vec<String> = Vec::with_capacity(streak_len);
        let mut window_ok = true;

        for pair in change_window.windows(2) {
            let prev = pair[0];
            let last = pair[1];
            if prev <= 0.0 {
                window_ok = false;
                break;
            }
            let change_pct = (last - prev) / prev * 100.0;
            if change_pct > -threshold {
                window_ok = false;
                break;
            }
            change_parts.push(format!("{:.2}%", change_pct));
        }

        if window_ok {
            return Some(change_parts);
        }
    }

    None
}

fn should_send_alert(
    alert_active_by_symbol: &mut HashMap<String, bool>,
    symbol: &str,
    is_match: bool,
) -> bool {
    let was_active = alert_active_by_symbol.get(symbol).copied().unwrap_or(false);
    alert_active_by_symbol.insert(symbol.to_string(), is_match);
    is_match && !was_active
}

fn send_momentum_alert(
    client: &reqwest::blocking::Client,
    config: &Config,
    symbol: &str,
    change_parts: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let request_ts = format_timestamp_de(Local::now());
    let base = symbol.strip_suffix("USDT").unwrap_or(symbol);
    let trade_url = format!("https://www.binance.com/de/trade/{}_USDT", base);
    let pair = format!("{}/USDT", base);
    let changes = change_parts.join(" > ");
    let config_info = format!(
        "Refresh: {}s\nLookback: {} candles\nThreshold: {:.2}%\nCandle Length: {}",
        config.interval_secs,
        config.lookback_candles,
        config.change_threshold_pct,
        config.candle_interval
    );
    let message = format!(
        "{}\n\n*{}*\n{}\n\n{}\n{}",
        escape_markdown_v2(&request_ts),
        escape_markdown_v2(&pair),
        escape_markdown_v2(&changes),
        escape_markdown_v2(&config_info),
        escape_markdown_v2(&trade_url)
    );
    send_telegram_message(
        client,
        &config.telegram_token,
        &config.telegram_chat_id,
        &message,
    )?;
    println!(
        "{}: Telegram momentum message sent for {}",
        format_timestamp_short(Local::now()),
        symbol
    );
    Ok(())
}

fn send_bearish_momentum_alert(
    client: &reqwest::blocking::Client,
    config: &Config,
    symbol: &str,
    change_parts: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let request_ts = format_timestamp_de(Local::now());
    let base = symbol.strip_suffix("USDT").unwrap_or(symbol);
    let trade_url = format!("https://www.binance.com/de/trade/{}_USDT", base);
    let pair = format!("{}/USDT", base);
    let changes = change_parts.join(" > ");
    let config_info = format!(
        "Bearish Momentum\nRefresh: {}s\nLookback: {} candles\nThreshold: -{:.2}%\nCandle Length: {}",
        config.interval_secs,
        config.lookback_candles,
        config.bearish_change_threshold_pct,
        config.candle_interval
    );
    let message = format!(
        "{}\n\n*{}*\n{}\n\n{}\n{}",
        escape_markdown_v2(&request_ts),
        escape_markdown_v2(&pair),
        escape_markdown_v2(&changes),
        escape_markdown_v2(&config_info),
        escape_markdown_v2(&trade_url)
    );
    send_telegram_message(
        client,
        &config.telegram_token,
        &config.telegram_chat_id,
        &message,
    )?;
    println!(
        "{}: Telegram bearish momentum message sent for {}",
        format_timestamp_short(Local::now()),
        symbol
    );
    Ok(())
}

fn send_listing_alert(
    client: &reqwest::blocking::Client,
    config: &Config,
    symbol: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let request_ts = format_timestamp_de(Local::now());
    let base = symbol.strip_suffix("USDT").unwrap_or(symbol);
    let trade_url = format!("https://www.binance.com/de/trade/{}_USDT", base);
    let pair = format!("{}/USDT", base);
    let listing_info = format!(
        "Fresh Listing\nPoll: {}s\nFresh Candle Length: {}\nFresh TTL: {}m",
        config.listing_poll_secs,
        config.fresh_listing_candle_interval,
        config.fresh_listing_ttl_mins
    );
    let message = format!(
        "{}\n\n*Neues Binance Listing erkannt*\n{}\n\n{}\n{}",
        escape_markdown_v2(&request_ts),
        escape_markdown_v2(&pair),
        escape_markdown_v2(&listing_info),
        escape_markdown_v2(&trade_url)
    );
    send_telegram_message(
        client,
        &config.telegram_token,
        &config.telegram_chat_id,
        &message,
    )?;
    println!(
        "{}: Telegram listing message sent for {}",
        format_timestamp_short(Local::now()),
        symbol
    );
    Ok(())
}

fn send_fresh_listing_momentum_alert(
    client: &reqwest::blocking::Client,
    config: &Config,
    symbol: &str,
    change_parts: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    let request_ts = format_timestamp_de(Local::now());
    let base = symbol.strip_suffix("USDT").unwrap_or(symbol);
    let trade_url = format!("https://www.binance.com/de/trade/{}_USDT", base);
    let pair = format!("{}/USDT", base);
    let changes = change_parts.join(" > ");
    let config_info = format!(
        "Fresh Listing Momentum\nPoll: {}s\nThreshold: {:.2}%\nCandle Length: {}",
        config.listing_poll_secs, config.change_threshold_pct, config.fresh_listing_candle_interval
    );
    let message = format!(
        "{}\n\n*{}*\n{}\n\n{}\n{}",
        escape_markdown_v2(&request_ts),
        escape_markdown_v2(&pair),
        escape_markdown_v2(&changes),
        escape_markdown_v2(&config_info),
        escape_markdown_v2(&trade_url)
    );
    send_telegram_message(
        client,
        &config.telegram_token,
        &config.telegram_chat_id,
        &message,
    )?;
    println!(
        "{}: Telegram fresh listing momentum message sent for {}",
        format_timestamp_short(Local::now()),
        symbol
    );
    Ok(())
}

fn register_new_listings(
    client: &reqwest::blocking::Client,
    config: &Config,
    current_trading_symbols: &[String],
    known_trading_symbols: &mut HashSet<String>,
    fresh_listings: &mut HashMap<String, FreshListingState>,
) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "{}: Diffing trading symbol set (previous={}, current={})",
        format_timestamp_short(Local::now()),
        known_trading_symbols.len(),
        current_trading_symbols.len()
    );

    let mut new_symbols = Vec::new();
    for symbol in current_trading_symbols {
        if known_trading_symbols.contains(symbol) {
            continue;
        }

        new_symbols.push(symbol.clone());
        println!(
            "{}: Detected new Binance listing {}",
            format_timestamp_short(Local::now()),
            symbol
        );
        send_listing_alert(client, config, symbol)?;
        fresh_listings.insert(symbol.clone(), FreshListingState::new());
    }

    if new_symbols.is_empty() {
        println!(
            "{}: Listing diff found no new symbols",
            format_timestamp_short(Local::now())
        );
    } else {
        println!(
            "{}: Listing diff found {} new symbol(s): {}",
            format_timestamp_short(Local::now()),
            new_symbols.len(),
            new_symbols.join(", ")
        );
    }

    known_trading_symbols.clear();
    known_trading_symbols.extend(current_trading_symbols.iter().cloned());
    Ok(())
}

fn prune_finished_fresh_listings(
    config: &Config,
    fresh_listings: &mut HashMap<String, FreshListingState>,
    known_trading_symbols: &HashSet<String>,
) {
    let max_age = Duration::from_secs(config.fresh_listing_ttl_mins.saturating_mul(60));
    fresh_listings.retain(|symbol, state| {
        let still_trading = known_trading_symbols.contains(symbol);
        let within_ttl = max_age.is_zero() || state.detected_at.elapsed() < max_age;
        let keep = still_trading && within_ttl;

        if !keep {
            println!(
                "{}: Removing {} from fresh listing tracking (still_trading={}, age_secs={}, closed_candles={})",
                format_timestamp_short(Local::now()),
                symbol,
                still_trading,
                state.detected_at.elapsed().as_secs(),
                state.last_candle_count
            );
        }

        keep
    });
}

fn refresh_fresh_listing_states(
    client: &reqwest::blocking::Client,
    config: &Config,
    fresh_listings: &mut HashMap<String, FreshListingState>,
) -> Result<(), Box<dyn std::error::Error>> {
    let required_closed_klines = config.streak_len.saturating_add(1);

    let tracked_symbols: Vec<String> = fresh_listings.keys().cloned().collect();
    for symbol in tracked_symbols {
        let Some(state) = fresh_listings.get_mut(&symbol) else {
            continue;
        };

        let klines = fetch_klines(
            client,
            &symbol,
            config.fresh_listing_candle_interval.as_str(),
            required_closed_klines + 1,
        )?;
        let closes = extract_closed_closes(klines, required_closed_klines);
        if closes.len() != state.last_candle_count {
            println!(
                "{}: {} fresh listing candles updated from {} to {}",
                format_timestamp_short(Local::now()),
                symbol,
                state.last_candle_count,
                closes.len()
            );
            state.last_candle_count = closes.len();
        }

        let is_match =
            find_matching_window(&closes, config.streak_len, config.change_threshold_pct);
        match is_match {
            Some(change_parts) => {
                if !state.momentum_alert_active {
                    send_fresh_listing_momentum_alert(client, config, &symbol, &change_parts)?;
                }
                state.momentum_alert_active = true;
            }
            None => {
                state.momentum_alert_active = false;
            }
        }
    }

    Ok(())
}

fn validate_config(config: &Config) -> Result<(), Box<dyn std::error::Error>> {
    if config.streak_len == 0 {
        return Err("--streak-len must be at least 1".into());
    }
    if config.lookback_candles < config.streak_len {
        return Err("--lookback-candles must be greater than or equal to --streak-len".into());
    }
    candle_interval_secs(&config.candle_interval)?;
    candle_interval_secs(&config.fresh_listing_candle_interval)?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (mut config, save_config) = parse_config()?;
    validate_config(&config)?;
    log_config(&config, "startup");
    if save_config {
        let path = save_config_file(&config)?;
        println!("Saved config to {}", path.display());
    }

    let client = reqwest::blocking::Client::new();
    let mut alert_active_by_symbol: HashMap<String, bool> = HashMap::new();
    let mut bearish_alert_active_by_symbol: HashMap<String, bool> = HashMap::new();
    let mut known_trading_symbols: HashSet<String> = HashSet::new();
    let mut fresh_listings: HashMap<String, FreshListingState> = HashMap::new();

    loop {
        if let Ok(Some(updated)) = try_reload_config() {
            validate_config(&updated)?;
            config = updated;
            log_config(&config, "reload");
        }

        println!(
            "{}: Refreshing symbol universe",
            format_timestamp_short(Local::now())
        );
        let universe = load_symbol_universe(&client, config.min_quote_volume_24h)?;
        if known_trading_symbols.is_empty() {
            known_trading_symbols.extend(universe.trading_usdt_symbols.iter().cloned());
            println!(
                "{}: Initialized trading symbol baseline with {} symbols",
                format_timestamp_short(Local::now()),
                known_trading_symbols.len()
            );
        } else {
            register_new_listings(
                &client,
                &config,
                &universe.trading_usdt_symbols,
                &mut known_trading_symbols,
                &mut fresh_listings,
            )?;
        }

        println!(
            "{}: Pairs above {} 24h quote volume: {}",
            format_timestamp_short(Local::now()),
            config.min_quote_volume_24h,
            universe.tracked_symbols.len()
        );
        if universe.tracked_symbols.is_empty() {
            return Err("no USDT trading symbols found above the configured 24h volume".into());
        }

        let tracked_symbol_set: HashSet<String> =
            universe.tracked_symbols.iter().cloned().collect();
        let required_closed_klines = config.lookback_candles + 1;
        let mut closes_by_symbol = seed_closes(
            &client,
            &universe.tracked_symbols,
            config.candle_interval.as_str(),
            required_closed_klines,
        )?;

        for symbol in &universe.tracked_symbols {
            let bullish_match = closes_by_symbol.get(symbol).and_then(|closes| {
                find_matching_window(closes, config.streak_len, config.change_threshold_pct)
            });
            let bearish_match = closes_by_symbol.get(symbol).and_then(|closes| {
                find_bearish_matching_window(
                    closes,
                    config.streak_len,
                    config.bearish_change_threshold_pct,
                )
            });

            match bullish_match {
                Some(change_parts) => {
                    if should_send_alert(&mut alert_active_by_symbol, symbol, true) {
                        println!(
                            "{}: {} matched immediately after seeding",
                            format_timestamp_short(Local::now()),
                            symbol
                        );
                        send_momentum_alert(&client, &config, symbol, &change_parts)?;
                    }
                }
                None => {
                    should_send_alert(&mut alert_active_by_symbol, symbol, false);
                }
            }

            match bearish_match {
                Some(change_parts) => {
                    if should_send_alert(&mut bearish_alert_active_by_symbol, symbol, true) {
                        println!(
                            "{}: {} matched bearish trend immediately after seeding",
                            format_timestamp_short(Local::now()),
                            symbol
                        );
                        send_bearish_momentum_alert(&client, &config, symbol, &change_parts)?;
                    }
                }
                None => {
                    should_send_alert(&mut bearish_alert_active_by_symbol, symbol, false);
                }
            }
        }
        alert_active_by_symbol.retain(|symbol, _| tracked_symbol_set.contains(symbol));
        bearish_alert_active_by_symbol.retain(|symbol, _| tracked_symbol_set.contains(symbol));

        let mut sockets = Vec::new();
        for batch in universe.tracked_symbols.chunks(WS_BATCH_SIZE) {
            let mut socket = connect_kline_stream(batch, config.candle_interval.as_str())?;
            set_socket_read_timeout(&mut socket, Duration::from_millis(250))?;
            println!(
                "{}: WebSocket connected for batch with {} symbols",
                format_timestamp_short(Local::now()),
                batch.len()
            );
            sockets.push(socket);
        }
        let refresh_at = Instant::now() + Duration::from_secs(config.interval_secs.max(1));
        let mut next_listing_poll_at = Instant::now();

        println!(
            "{}: WebSocket connected for {} symbols across {} batches",
            format_timestamp_short(Local::now()),
            universe.tracked_symbols.len(),
            sockets.len()
        );

        while Instant::now() < refresh_at {
            if Instant::now() >= next_listing_poll_at {
                println!(
                    "{}: Polling exchangeInfo for new listings",
                    format_timestamp_short(Local::now())
                );
                let trading_symbols = fetch_trading_usdt_symbols(&client)?;
                register_new_listings(
                    &client,
                    &config,
                    &trading_symbols,
                    &mut known_trading_symbols,
                    &mut fresh_listings,
                )?;
                refresh_fresh_listing_states(&client, &config, &mut fresh_listings)?;
                prune_finished_fresh_listings(&config, &mut fresh_listings, &known_trading_symbols);
                next_listing_poll_at =
                    Instant::now() + Duration::from_secs(config.listing_poll_secs.max(1));
            }

            for socket in &mut sockets {
                let message = match socket.read() {
                    Ok(message) => message,
                    Err(tungstenite::Error::Io(err))
                        if err.kind() == ErrorKind::WouldBlock
                            || err.kind() == ErrorKind::TimedOut =>
                    {
                        continue;
                    }
                    Err(err) => return Err(err.into()),
                };
                let Message::Text(text) = message else {
                    continue;
                };

                if text.contains("\"result\"") {
                    if let Ok(ack) = serde_json::from_str::<WsSubscriptionAck>(&text) {
                        println!(
                            "{}: WebSocket subscription acknowledged (id={})",
                            format_timestamp_short(Local::now()),
                            ack.id.unwrap_or_default()
                        );
                        continue;
                    }
                }

                let event: WsKlineEvent = match serde_json::from_str(&text) {
                    Ok(value) => value,
                    Err(_) => continue,
                };
                if !event.kline.is_closed {
                    continue;
                }

                let close = match event.kline.close.parse::<f64>() {
                    Ok(value) => value,
                    Err(_) => continue,
                };

                let symbol = event.symbol;
                let closes = closes_by_symbol
                    .entry(symbol.clone())
                    .or_insert_with(|| VecDeque::with_capacity(required_closed_klines));
                if closes.len() == required_closed_klines {
                    closes.pop_front();
                }
                closes.push_back(close);

                println!(
                    "{}: {} received closed kline (have {}, need {})",
                    format_timestamp_short(Local::now()),
                    symbol,
                    closes.len(),
                    required_closed_klines
                );

                if closes.len() < required_closed_klines {
                    println!(
                        "{}: {} insufficient closed klines (have {}, need {})",
                        format_timestamp_short(Local::now()),
                        symbol,
                        closes.len(),
                        required_closed_klines
                    );
                    continue;
                }

                let bullish_match =
                    find_matching_window(closes, config.streak_len, config.change_threshold_pct);
                let bearish_match = find_bearish_matching_window(
                    closes,
                    config.streak_len,
                    config.bearish_change_threshold_pct,
                );

                match bullish_match {
                    Some(change_parts) => {
                        if should_send_alert(&mut alert_active_by_symbol, &symbol, true) {
                            send_momentum_alert(&client, &config, &symbol, &change_parts)?;
                        }
                    }
                    None => {
                        should_send_alert(&mut alert_active_by_symbol, &symbol, false);
                    }
                }

                match bearish_match {
                    Some(change_parts) => {
                        if should_send_alert(&mut bearish_alert_active_by_symbol, &symbol, true) {
                            send_bearish_momentum_alert(&client, &config, &symbol, &change_parts)?;
                        }
                    }
                    None => {
                        should_send_alert(&mut bearish_alert_active_by_symbol, &symbol, false);
                    }
                }
            }
        }

        println!(
            "{}: Refresh window ended, reconnecting WebSocket",
            format_timestamp_short(Local::now())
        );
    }
}

#[cfg(test)]
mod tests {
    use super::{find_bearish_matching_window, find_matching_window, should_send_alert};
    use std::collections::{HashMap, VecDeque};

    fn closes(values: &[f64]) -> VecDeque<f64> {
        values.iter().copied().collect()
    }

    #[test]
    fn bullish_matching_window_still_matches_rising_sequence() {
        let result = find_matching_window(&closes(&[100.0, 101.0, 102.5]), 2, 0.5);
        assert_eq!(result, Some(vec!["1.00%".to_string(), "1.49%".to_string()]));
    }

    #[test]
    fn bearish_matching_window_matches_falling_sequence() {
        let result = find_bearish_matching_window(&closes(&[100.0, 99.0, 98.0]), 2, 0.5);
        assert_eq!(
            result,
            Some(vec!["-1.00%".to_string(), "-1.01%".to_string()])
        );
    }

    #[test]
    fn bearish_matching_window_rejects_mixed_sequence() {
        let result = find_bearish_matching_window(&closes(&[100.0, 99.0, 99.5]), 2, 0.5);
        assert_eq!(result, None);
    }

    #[test]
    fn bearish_matching_window_rejects_weak_or_flat_sequence() {
        let result = find_bearish_matching_window(&closes(&[100.0, 99.7, 99.5]), 2, 0.5);
        assert_eq!(result, None);
    }

    #[test]
    fn bearish_matching_window_rejects_non_positive_previous_close() {
        let result = find_bearish_matching_window(&closes(&[0.0, -1.0, -2.0]), 2, 0.5);
        assert_eq!(result, None);
    }

    #[test]
    fn alert_state_allows_independent_bullish_and_bearish_tracking() {
        let mut bullish = HashMap::new();
        let mut bearish = HashMap::new();

        assert!(should_send_alert(&mut bullish, "BTCUSDT", true));
        assert!(!should_send_alert(&mut bullish, "BTCUSDT", true));
        assert!(!should_send_alert(&mut bearish, "BTCUSDT", false));
        assert!(should_send_alert(&mut bearish, "BTCUSDT", true));
        assert!(!should_send_alert(&mut bearish, "BTCUSDT", true));
    }

    #[test]
    fn alert_state_resets_when_direction_changes() {
        let mut bullish = HashMap::new();
        let mut bearish = HashMap::new();

        assert!(should_send_alert(&mut bullish, "ETHUSDT", true));
        assert!(!should_send_alert(&mut bullish, "ETHUSDT", false));
        assert!(should_send_alert(&mut bearish, "ETHUSDT", true));
    }
}
