mod binance;
mod config;
mod detector;
mod healthcheck;
mod state;
mod telegram;
mod time_utils;

use binance::{
    KlineSocket, WsKlineEvent, WsSubscriptionAck, connect_kline_stream, fetch_closed_candles,
    fetch_exchange_info, fetch_quote_volumes_24h, set_socket_read_timeout,
};
use chrono::Local;
use config::{
    Config, ValidatedConfig, config_modified_time, parse_config, reload_config, save_config_file,
};
use detector::{CandidateResult, Candle, DetectorSettings, detect_candidate};
use healthcheck::call_healthcheck;
use state::AlertState;
use std::collections::{HashMap, HashSet};
use std::io::ErrorKind;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use telegram::{candidate_message, send_candidate_alert, send_candidate_summaries};
use time_utils::format_timestamp_short;
use tungstenite::Message;

const WS_BATCH_SIZE: usize = 100;
const CONFIG_POLL_SECS: u64 = 5;
const HEALTHCHECK_INTERVAL_SECS: u64 = 60 * 60;

#[derive(Debug)]
struct SymbolUniverse {
    symbols: Vec<String>,
    quote_volumes: HashMap<String, f64>,
}

fn now_log() -> String {
    format_timestamp_short(Local::now())
}

fn log_config(config: &Config, validated: &ValidatedConfig, source: &str, dry_run: bool) {
    println!(
        "{}: Config ({source}): window={} ({} candles), interval={}, band=+/-{:.2}%, min_in_band={:.2}%, spike=+{:.2}%, quiet={} ({} candles), quote_volume_24h={:.0}..={:.0}, symbol_refresh_secs={}, dry_run={dry_run}",
        now_log(),
        config.rolling_window,
        validated.window_candles,
        config.candle_interval,
        config.sideways_band_pct,
        config.min_sideways_closes_pct,
        config.positive_spike_pct,
        config.post_spike_quiet_period,
        validated.quiet_candles,
        config.min_quote_volume_24h,
        config.max_quote_volume_24h,
        config.symbol_refresh_secs,
    );
}

fn load_symbol_universe(
    client: &reqwest::blocking::Client,
    min_quote_volume_24h: f64,
    max_quote_volume_24h: f64,
) -> Result<SymbolUniverse, Box<dyn std::error::Error>> {
    let info = fetch_exchange_info(client)?;
    let volumes = fetch_quote_volumes_24h(client)?;
    let symbols = select_tracked_symbols(
        info.symbols,
        &volumes,
        min_quote_volume_24h,
        max_quote_volume_24h,
    );
    let quote_volumes = symbols
        .iter()
        .filter_map(|symbol| volumes.get(symbol).map(|volume| (symbol.clone(), *volume)))
        .collect();
    println!(
        "{}: Tracking {} trading USDT symbols with 24h quote volume {:.0}..={:.0}",
        now_log(),
        symbols.len(),
        min_quote_volume_24h,
        max_quote_volume_24h
    );
    if symbols.is_empty() {
        return Err("no trading USDT symbols meet the configured 24h quote-volume range".into());
    }
    Ok(SymbolUniverse {
        symbols,
        quote_volumes,
    })
}

fn select_tracked_symbols(
    symbols: Vec<binance::SymbolInfo>,
    volumes: &HashMap<String, f64>,
    min_quote_volume_24h: f64,
    max_quote_volume_24h: f64,
) -> Vec<String> {
    let mut tracked: Vec<String> = symbols
        .into_iter()
        .filter(|symbol| symbol.status == "TRADING" && symbol.quote_asset == "USDT")
        .filter(|symbol| {
            volumes.get(&symbol.symbol).is_some_and(|volume| {
                *volume >= min_quote_volume_24h && *volume <= max_quote_volume_24h
            })
        })
        .map(|symbol| symbol.symbol)
        .collect();
    tracked.sort();
    tracked
}

fn seed_symbols(
    client: &reqwest::blocking::Client,
    symbols: &[String],
    interval: &str,
    window_candles: usize,
    histories: &mut HashMap<String, Vec<Candle>>,
) -> Result<(), Box<dyn std::error::Error>> {
    for symbol in symbols {
        println!(
            "{}: Loading {} closed {} candles for {}",
            now_log(),
            window_candles,
            interval,
            symbol
        );
        let candles = fetch_closed_candles(client, symbol, interval, window_candles)?;
        if candles.len() < window_candles {
            println!(
                "{}: {} has insufficient history ({}/{}) and cannot qualify yet",
                now_log(),
                symbol,
                candles.len(),
                window_candles
            );
        }
        histories.insert(symbol.clone(), candles);
    }
    Ok(())
}

fn detector_settings(config: &Config, validated: &ValidatedConfig) -> DetectorSettings {
    DetectorSettings {
        window_candles: validated.window_candles,
        interval_ms: validated.interval_ms,
        sideways_band_pct: config.sideways_band_pct,
        min_sideways_closes_pct: config.min_sideways_closes_pct,
        positive_spike_pct: config.positive_spike_pct,
        quiet_period_ms: validated.quiet_period_ms,
    }
}

fn evaluate_symbol(
    symbol: &str,
    histories: &HashMap<String, Vec<Candle>>,
    quote_volumes: &HashMap<String, f64>,
    config: &Config,
    validated: &ValidatedConfig,
) -> Option<CandidateResult> {
    let candles = histories.get(symbol)?;
    detect_candidate(
        symbol,
        candles,
        detector_settings(config, validated),
        quote_volumes.get(symbol).copied().unwrap_or_default(),
    )
}

fn evaluate_all(
    universe: &SymbolUniverse,
    histories: &HashMap<String, Vec<Candle>>,
    config: &Config,
    validated: &ValidatedConfig,
) -> Vec<CandidateResult> {
    universe
        .symbols
        .iter()
        .filter_map(|symbol| {
            evaluate_symbol(
                symbol,
                histories,
                &universe.quote_volumes,
                config,
                validated,
            )
        })
        .collect()
}

fn alert_startup_candidates(
    client: &reqwest::blocking::Client,
    config: &Config,
    dry_run: bool,
    state: &mut AlertState,
    candidates: &[CandidateResult],
) -> Result<(), Box<dyn std::error::Error>> {
    let unseen: Vec<CandidateResult> = candidates
        .iter()
        .filter(|candidate| !state.is_alerted(candidate, &config.candle_interval))
        .cloned()
        .collect();
    println!(
        "{}: Startup scan found {} candidates ({} not previously reported)",
        now_log(),
        candidates.len(),
        unseen.len()
    );
    if unseen.is_empty() {
        return Ok(());
    }
    if dry_run {
        for candidate in &unseen {
            println!("{}: DRY RUN candidate: {candidate:#?}", now_log());
        }
        return Ok(());
    }
    send_candidate_summaries(client, config, &unseen)?;
    for candidate in &unseen {
        state.mark_alerted(candidate, &config.candle_interval);
    }
    state.save()?;
    Ok(())
}

fn alert_live_candidate(
    client: &reqwest::blocking::Client,
    config: &Config,
    dry_run: bool,
    state: &mut AlertState,
    candidate: &CandidateResult,
) -> Result<(), Box<dyn std::error::Error>> {
    if state.is_alerted(candidate, &config.candle_interval) {
        return Ok(());
    }
    if dry_run {
        println!(
            "{}: DRY RUN live candidate:\n{}",
            now_log(),
            candidate_message(config, candidate)
        );
        return Ok(());
    }
    send_candidate_alert(client, config, candidate)?;
    state.mark_alerted(candidate, &config.candle_interval);
    state.save()?;
    println!(
        "{}: Telegram candidate message sent for {}",
        now_log(),
        candidate.symbol
    );
    Ok(())
}

fn connect_all_sockets(
    symbols: &[String],
    interval: &str,
) -> Result<Vec<KlineSocket>, Box<dyn std::error::Error>> {
    let mut sockets = Vec::new();
    for batch in symbols.chunks(WS_BATCH_SIZE) {
        let mut socket = connect_kline_stream(batch, interval)?;
        set_socket_read_timeout(&mut socket, Duration::from_millis(100))?;
        sockets.push(socket);
    }
    println!(
        "{}: Connected {} WebSocket batches for {} symbols",
        now_log(),
        sockets.len(),
        symbols.len()
    );
    Ok(sockets)
}

fn connect_sockets_with_backoff(
    symbols: &[String],
    interval: &str,
    backoff_secs: &mut u64,
) -> Vec<KlineSocket> {
    loop {
        match connect_all_sockets(symbols, interval) {
            Ok(sockets) => return sockets,
            Err(error) => {
                eprintln!(
                    "{}: WebSocket connection failed: {error}; retrying in {}s",
                    now_log(),
                    *backoff_secs
                );
                thread::sleep(Duration::from_secs(*backoff_secs));
                *backoff_secs = (*backoff_secs * 2).min(60);
            }
        }
    }
}

fn same_symbols(left: &[String], right: &[String]) -> bool {
    left == right
}

fn analysis_config_changed(left: &Config, right: &Config) -> bool {
    left.rolling_window != right.rolling_window
        || left.candle_interval != right.candle_interval
        || left.sideways_band_pct != right.sideways_band_pct
        || left.min_sideways_closes_pct != right.min_sideways_closes_pct
        || left.positive_spike_pct != right.positive_spike_pct
        || left.post_spike_quiet_period != right.post_spike_quiet_period
        || left.min_quote_volume_24h != right.min_quote_volume_24h
        || left.max_quote_volume_24h != right.max_quote_volume_24h
        || left.symbol_refresh_secs != right.symbol_refresh_secs
}

fn unix_time_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

fn perform_healthcheck(client: &reqwest::blocking::Client, url: &str, dry_run: bool) {
    if url.is_empty() {
        return;
    }
    if dry_run {
        println!("{}: DRY RUN healthcheck skipped", now_log());
        return;
    }
    let client = client.clone();
    let url = url.to_string();
    thread::spawn(move || match call_healthcheck(&client, &url) {
        Ok(status) => println!("{}: Healthcheck succeeded ({status})", now_log()),
        Err(error) => eprintln!("{}: Healthcheck failed: {error}", now_log()),
    });
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let parsed = parse_config()?;
    let mut config = parsed.config;
    let mut validated = parsed.validated;
    let dry_run = parsed.dry_run;
    log_config(&config, &validated, "startup", dry_run);
    if parsed.save_config {
        let path = save_config_file(&config)?;
        println!("{}: Saved config to {}", now_log(), path.display());
    }

    let client = reqwest::blocking::Client::builder()
        .timeout(Duration::from_secs(20))
        .build()?;
    perform_healthcheck(&client, &config.healthcheck_url, dry_run);
    let mut next_healthcheck = Instant::now() + Duration::from_secs(HEALTHCHECK_INTERVAL_SECS);
    let mut alert_state = AlertState::load()?;
    alert_state.prune(unix_time_ms() - validated.window_ms);
    if !dry_run {
        alert_state.save()?;
    }
    let mut last_config_modified = config_modified_time()?;
    let mut reconnect_backoff_secs = 1_u64;

    'analysis_generation: loop {
        let mut universe = load_symbol_universe(
            &client,
            config.min_quote_volume_24h,
            config.max_quote_volume_24h,
        )?;
        let mut histories = HashMap::with_capacity(universe.symbols.len());
        seed_symbols(
            &client,
            &universe.symbols,
            &config.candle_interval,
            validated.window_candles,
            &mut histories,
        )?;
        let startup_candidates = evaluate_all(&universe, &histories, &config, &validated);
        alert_startup_candidates(
            &client,
            &config,
            dry_run,
            &mut alert_state,
            &startup_candidates,
        )?;

        let mut sockets = connect_sockets_with_backoff(
            &universe.symbols,
            &config.candle_interval,
            &mut reconnect_backoff_secs,
        );
        let mut next_symbol_refresh =
            Instant::now() + Duration::from_secs(config.symbol_refresh_secs);
        let mut next_config_poll = Instant::now() + Duration::from_secs(CONFIG_POLL_SECS);

        loop {
            if Instant::now() >= next_healthcheck {
                perform_healthcheck(&client, &config.healthcheck_url, dry_run);
                next_healthcheck = Instant::now() + Duration::from_secs(HEALTHCHECK_INTERVAL_SECS);
            }

            if Instant::now() >= next_config_poll {
                let modified = config_modified_time()?;
                if modified != last_config_modified {
                    match reload_config(dry_run) {
                        Ok(Some((updated, updated_validated))) => {
                            let restart_analysis = analysis_config_changed(&config, &updated);
                            let healthcheck_changed =
                                config.healthcheck_url != updated.healthcheck_url;
                            config = updated;
                            validated = updated_validated;
                            log_config(&config, &validated, "reload", dry_run);
                            last_config_modified = modified;
                            if healthcheck_changed {
                                perform_healthcheck(&client, &config.healthcheck_url, dry_run);
                                next_healthcheck =
                                    Instant::now() + Duration::from_secs(HEALTHCHECK_INTERVAL_SECS);
                            }
                            if restart_analysis {
                                continue 'analysis_generation;
                            }
                        }
                        Ok(None) => last_config_modified = modified,
                        Err(error) => {
                            eprintln!("{}: Ignoring invalid reloaded config: {error}", now_log());
                            last_config_modified = modified;
                        }
                    }
                }
                next_config_poll = Instant::now() + Duration::from_secs(CONFIG_POLL_SECS);
            }

            if Instant::now() >= next_symbol_refresh {
                let updated = load_symbol_universe(
                    &client,
                    config.min_quote_volume_24h,
                    config.max_quote_volume_24h,
                )?;
                if !same_symbols(&universe.symbols, &updated.symbols) {
                    let previous: HashSet<&str> =
                        universe.symbols.iter().map(String::as_str).collect();
                    let added: Vec<String> = updated
                        .symbols
                        .iter()
                        .filter(|symbol| !previous.contains(symbol.as_str()))
                        .cloned()
                        .collect();
                    let removed_count = universe
                        .symbols
                        .iter()
                        .filter(|symbol| !updated.symbols.contains(symbol))
                        .count();
                    let updated_set: HashSet<&str> =
                        updated.symbols.iter().map(String::as_str).collect();
                    histories.retain(|symbol, _| updated_set.contains(symbol.as_str()));
                    seed_symbols(
                        &client,
                        &added,
                        &config.candle_interval,
                        validated.window_candles,
                        &mut histories,
                    )?;
                    universe = updated;
                    sockets = connect_sockets_with_backoff(
                        &universe.symbols,
                        &config.candle_interval,
                        &mut reconnect_backoff_secs,
                    );
                    println!(
                        "{}: Applied symbol universe change ({} added, {} removed)",
                        now_log(),
                        added.len(),
                        removed_count
                    );
                    for candidate in evaluate_all(&universe, &histories, &config, &validated) {
                        alert_live_candidate(
                            &client,
                            &config,
                            dry_run,
                            &mut alert_state,
                            &candidate,
                        )?;
                    }
                } else {
                    universe.quote_volumes = updated.quote_volumes;
                }
                alert_state.prune(unix_time_ms() - validated.window_ms);
                if !dry_run {
                    alert_state.save()?;
                }
                next_symbol_refresh =
                    Instant::now() + Duration::from_secs(config.symbol_refresh_secs);
            }

            let mut reconnect_required = false;
            for socket in &mut sockets {
                let message = match socket.read() {
                    Ok(message) => message,
                    Err(tungstenite::Error::Io(error))
                        if error.kind() == ErrorKind::WouldBlock
                            || error.kind() == ErrorKind::TimedOut =>
                    {
                        continue;
                    }
                    Err(error) => {
                        eprintln!("{}: WebSocket read failed: {error}", now_log());
                        reconnect_required = true;
                        break;
                    }
                };
                reconnect_backoff_secs = 1;
                let Message::Text(text) = message else {
                    continue;
                };
                if text.contains("\"result\"")
                    && let Ok(ack) = serde_json::from_str::<WsSubscriptionAck>(&text)
                {
                    println!(
                        "{}: WebSocket subscription acknowledged (id={})",
                        now_log(),
                        ack.id.unwrap_or_default()
                    );
                    continue;
                }
                let event: WsKlineEvent = match serde_json::from_str(&text) {
                    Ok(event) => event,
                    Err(_) => continue,
                };
                if !event.kline.is_closed {
                    continue;
                }
                let symbol = event.symbol;
                let Some(candle) = event.kline.into_candle() else {
                    continue;
                };
                let history = histories.entry(symbol.clone()).or_default();
                let has_gap = history.last().is_some_and(|last| {
                    candle.open_time_ms > last.open_time_ms
                        && candle.open_time_ms - last.open_time_ms != validated.interval_ms
                });
                if has_gap {
                    println!(
                        "{}: Gap detected for {}; refilling history",
                        now_log(),
                        symbol
                    );
                    *history = fetch_closed_candles(
                        &client,
                        &symbol,
                        &config.candle_interval,
                        validated.window_candles,
                    )?;
                } else if let Some(last) = history.last_mut()
                    && last.open_time_ms == candle.open_time_ms
                {
                    *last = candle;
                } else if history
                    .last()
                    .is_none_or(|last| candle.open_time_ms > last.open_time_ms)
                {
                    history.push(candle);
                    if history.len() > validated.window_candles {
                        history.remove(0);
                    }
                }

                if let Some(candidate) = evaluate_symbol(
                    &symbol,
                    &histories,
                    &universe.quote_volumes,
                    &config,
                    &validated,
                ) {
                    alert_live_candidate(&client, &config, dry_run, &mut alert_state, &candidate)?;
                }
            }

            if reconnect_required {
                eprintln!(
                    "{}: Reconnecting streams and refilling histories in {}s",
                    now_log(),
                    reconnect_backoff_secs
                );
                thread::sleep(Duration::from_secs(reconnect_backoff_secs));
                reconnect_backoff_secs = (reconnect_backoff_secs * 2).min(60);
                seed_symbols(
                    &client,
                    &universe.symbols,
                    &config.candle_interval,
                    validated.window_candles,
                    &mut histories,
                )?;
                sockets = connect_sockets_with_backoff(
                    &universe.symbols,
                    &config.candle_interval,
                    &mut reconnect_backoff_secs,
                );
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use binance::SymbolInfo;

    #[test]
    fn selects_only_liquid_trading_usdt_symbols() {
        let symbols = vec![
            SymbolInfo {
                symbol: "BTCUSDT".into(),
                status: "TRADING".into(),
                quote_asset: "USDT".into(),
            },
            SymbolInfo {
                symbol: "LOWUSDT".into(),
                status: "TRADING".into(),
                quote_asset: "USDT".into(),
            },
            SymbolInfo {
                symbol: "ETHBTC".into(),
                status: "TRADING".into(),
                quote_asset: "BTC".into(),
            },
            SymbolInfo {
                symbol: "OLDUSDT".into(),
                status: "BREAK".into(),
                quote_asset: "USDT".into(),
            },
        ];
        let volumes = HashMap::from([
            ("BTCUSDT".to_string(), 1_000.0),
            ("LOWUSDT".to_string(), 99.0),
            ("ETHBTC".to_string(), 2_000.0),
            ("OLDUSDT".to_string(), 2_000.0),
        ]);

        assert_eq!(
            select_tracked_symbols(symbols, &volumes, 100.0, 1_500.0),
            vec!["BTCUSDT"]
        );
    }

    #[test]
    fn excludes_symbols_above_maximum_quote_volume() {
        let symbols = vec![
            SymbolInfo {
                symbol: "BTCUSDT".into(),
                status: "TRADING".into(),
                quote_asset: "USDT".into(),
            },
            SymbolInfo {
                symbol: "BICOUSDT".into(),
                status: "TRADING".into(),
                quote_asset: "USDT".into(),
            },
        ];
        let volumes = HashMap::from([
            ("BTCUSDT".to_string(), 2_000.0),
            ("BICOUSDT".to_string(), 500.0),
        ]);

        assert_eq!(
            select_tracked_symbols(symbols, &volumes, 100.0, 1_000.0),
            vec!["BICOUSDT"]
        );
    }
}
