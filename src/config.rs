use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::SystemTime;

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    pub rolling_window: String,
    pub candle_interval: String,
    pub sideways_band_pct: f64,
    pub min_sideways_closes_pct: f64,
    pub positive_spike_pct: f64,
    pub post_spike_quiet_period: String,
    pub min_quote_volume_24h: f64,
    pub max_quote_volume_24h: f64,
    pub symbol_refresh_secs: u64,
    pub telegram_token: String,
    pub telegram_chat_id: String,
    pub healthcheck_url: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            rolling_window: "30d".to_string(),
            candle_interval: "1h".to_string(),
            sideways_band_pct: 2.0,
            min_sideways_closes_pct: 95.0,
            positive_spike_pct: 5.0,
            post_spike_quiet_period: "3d".to_string(),
            min_quote_volume_24h: 100_000_000.0,
            max_quote_volume_24h: 1_000_000_000.0,
            symbol_refresh_secs: 900,
            telegram_token: String::new(),
            telegram_chat_id: String::new(),
            healthcheck_url: String::new(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct ValidatedConfig {
    pub window_candles: usize,
    pub quiet_candles: usize,
    pub interval_ms: i64,
    pub window_ms: i64,
    pub quiet_period_ms: i64,
}

#[derive(Default)]
struct CliOverrides {
    rolling_window: Option<String>,
    candle_interval: Option<String>,
    sideways_band_pct: Option<f64>,
    min_sideways_closes_pct: Option<f64>,
    positive_spike_pct: Option<f64>,
    post_spike_quiet_period: Option<String>,
    min_quote_volume_24h: Option<f64>,
    max_quote_volume_24h: Option<f64>,
    symbol_refresh_secs: Option<u64>,
    telegram_token: Option<String>,
    telegram_chat_id: Option<String>,
    healthcheck_url: Option<String>,
}

impl CliOverrides {
    fn apply(&self, config: &mut Config) {
        if let Some(value) = &self.rolling_window {
            config.rolling_window.clone_from(value);
        }
        if let Some(value) = &self.candle_interval {
            config.candle_interval.clone_from(value);
        }
        if let Some(value) = self.sideways_band_pct {
            config.sideways_band_pct = value;
        }
        if let Some(value) = self.min_sideways_closes_pct {
            config.min_sideways_closes_pct = value;
        }
        if let Some(value) = self.positive_spike_pct {
            config.positive_spike_pct = value;
        }
        if let Some(value) = &self.post_spike_quiet_period {
            config.post_spike_quiet_period.clone_from(value);
        }
        if let Some(value) = self.min_quote_volume_24h {
            config.min_quote_volume_24h = value;
        }
        if let Some(value) = self.max_quote_volume_24h {
            config.max_quote_volume_24h = value;
        }
        if let Some(value) = self.symbol_refresh_secs {
            config.symbol_refresh_secs = value;
        }
        if let Some(value) = &self.telegram_token {
            config.telegram_token.clone_from(value);
        }
        if let Some(value) = &self.telegram_chat_id {
            config.telegram_chat_id.clone_from(value);
        }
        if let Some(value) = &self.healthcheck_url {
            config.healthcheck_url.clone_from(value);
        }
    }
}

pub struct ParsedConfig {
    pub config: Config,
    pub validated: ValidatedConfig,
    pub dry_run: bool,
    pub save_config: bool,
}

fn duration_secs(value: &str) -> Result<u64, Box<dyn std::error::Error>> {
    if value.len() < 2 {
        return Err(format!("invalid duration: {value}").into());
    }
    let (amount, unit) = value.split_at(value.len() - 1);
    let amount: u64 = amount.parse()?;
    if amount == 0 {
        return Err(format!("duration must be positive: {value}").into());
    }
    let multiplier = match unit {
        "m" => 60,
        "h" => 60 * 60,
        "d" => 60 * 60 * 24,
        "w" => 60 * 60 * 24 * 7,
        _ => return Err(format!("unsupported duration unit in {value}; use m, h, d or w").into()),
    };
    amount
        .checked_mul(multiplier)
        .ok_or_else(|| format!("duration is too large: {value}").into())
}

pub fn candle_interval_secs(interval: &str) -> Result<u64, Box<dyn std::error::Error>> {
    let seconds = match interval {
        "1s" => 1,
        "1m" => 60,
        "3m" => 3 * 60,
        "5m" => 5 * 60,
        "15m" => 15 * 60,
        "30m" => 30 * 60,
        "1h" => 60 * 60,
        "2h" => 2 * 60 * 60,
        "4h" => 4 * 60 * 60,
        "6h" => 6 * 60 * 60,
        "8h" => 8 * 60 * 60,
        "12h" => 12 * 60 * 60,
        "1d" => 24 * 60 * 60,
        "3d" => 3 * 24 * 60 * 60,
        "1w" => 7 * 24 * 60 * 60,
        "1M" => {
            return Err(
                "candle interval 1M is unsupported because calendar months vary in length".into(),
            );
        }
        _ => return Err(format!("unsupported Binance candle interval: {interval}").into()),
    };
    Ok(seconds)
}

pub fn validate_config(
    config: &Config,
    dry_run: bool,
) -> Result<ValidatedConfig, Box<dyn std::error::Error>> {
    let window_secs = duration_secs(&config.rolling_window)?;
    let quiet_secs = duration_secs(&config.post_spike_quiet_period)?;
    let interval_secs = candle_interval_secs(&config.candle_interval)?;

    if window_secs % interval_secs != 0 {
        return Err("rolling_window must be exactly divisible by candle_interval".into());
    }
    if quiet_secs % interval_secs != 0 {
        return Err("post_spike_quiet_period must be exactly divisible by candle_interval".into());
    }
    if quiet_secs >= window_secs {
        return Err("post_spike_quiet_period must be shorter than rolling_window".into());
    }
    let window_candles = usize::try_from(window_secs / interval_secs)?;
    let quiet_candles = usize::try_from(quiet_secs / interval_secs)?;
    if !(20..=10_000).contains(&window_candles) {
        return Err(format!(
            "rolling window must contain between 20 and 10000 candles, got {window_candles}"
        )
        .into());
    }
    if !(0.0..100.0).contains(&config.sideways_band_pct) {
        return Err("sideways_band_pct must be at least 0 and below 100".into());
    }
    if !(0.0..=100.0).contains(&config.min_sideways_closes_pct)
        || config.min_sideways_closes_pct == 0.0
    {
        return Err("min_sideways_closes_pct must be above 0 and at most 100".into());
    }
    if config.positive_spike_pct <= config.sideways_band_pct {
        return Err("positive_spike_pct must be greater than sideways_band_pct".into());
    }
    if !config.min_quote_volume_24h.is_finite() || config.min_quote_volume_24h < 0.0 {
        return Err("min_quote_volume_24h must be a finite, non-negative number".into());
    }
    if !config.max_quote_volume_24h.is_finite()
        || config.max_quote_volume_24h < config.min_quote_volume_24h
    {
        return Err(
            "max_quote_volume_24h must be finite and greater than or equal to min_quote_volume_24h"
                .into(),
        );
    }
    if config.symbol_refresh_secs == 0 {
        return Err("symbol_refresh_secs must be at least 1".into());
    }
    if !config.healthcheck_url.is_empty() {
        let url = reqwest::Url::parse(&config.healthcheck_url)
            .map_err(|error| format!("invalid healthcheck_url: {error}"))?;
        if !matches!(url.scheme(), "http" | "https") {
            return Err("healthcheck_url must use http or https".into());
        }
    }
    if !dry_run && (config.telegram_token.is_empty() || config.telegram_chat_id.is_empty()) {
        return Err("missing Telegram credentials; set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID or use --dry-run".into());
    }

    Ok(ValidatedConfig {
        window_candles,
        quiet_candles,
        interval_ms: i64::try_from(
            interval_secs
                .checked_mul(1000)
                .ok_or("candle interval is too large")?,
        )?,
        window_ms: i64::try_from(
            window_secs
                .checked_mul(1000)
                .ok_or("rolling window is too large")?,
        )?,
        quiet_period_ms: i64::try_from(
            quiet_secs
                .checked_mul(1000)
                .ok_or("quiet period is too large")?,
        )?,
    })
}

pub fn parse_config() -> Result<ParsedConfig, Box<dyn std::error::Error>> {
    let mut overrides = CliOverrides::default();
    let mut dry_run = false;
    let mut save_config = false;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        let value = |args: &mut std::iter::Skip<std::env::Args>, name: &str| {
            args.next()
                .ok_or_else(|| format!("missing value for {name}"))
        };
        match arg.as_str() {
            "--rolling-window" => overrides.rolling_window = Some(value(&mut args, &arg)?),
            "--candle-interval" => overrides.candle_interval = Some(value(&mut args, &arg)?),
            "--sideways-band-pct" => {
                overrides.sideways_band_pct = Some(value(&mut args, &arg)?.parse()?)
            }
            "--min-sideways-closes-pct" => {
                overrides.min_sideways_closes_pct = Some(value(&mut args, &arg)?.parse()?)
            }
            "--positive-spike-pct" => {
                overrides.positive_spike_pct = Some(value(&mut args, &arg)?.parse()?)
            }
            "--post-spike-quiet-period" => {
                overrides.post_spike_quiet_period = Some(value(&mut args, &arg)?)
            }
            "--min-quote-volume-24h" => {
                overrides.min_quote_volume_24h = Some(value(&mut args, &arg)?.parse()?)
            }
            "--max-quote-volume-24h" => {
                overrides.max_quote_volume_24h = Some(value(&mut args, &arg)?.parse()?)
            }
            "--symbol-refresh-secs" => {
                overrides.symbol_refresh_secs = Some(value(&mut args, &arg)?.parse()?)
            }
            "--telegram-token" => overrides.telegram_token = Some(value(&mut args, &arg)?),
            "--telegram-chat-id" => overrides.telegram_chat_id = Some(value(&mut args, &arg)?),
            "--healthcheck-url" => overrides.healthcheck_url = Some(value(&mut args, &arg)?),
            "--dry-run" => dry_run = true,
            "--save-config" => save_config = true,
            "--help" | "-h" => {
                print_help();
                std::process::exit(0);
            }
            _ => return Err(format!("unknown argument: {arg}").into()),
        }
    }

    let mut config = load_config_file()?.unwrap_or_default();
    apply_env_overrides(&mut config);
    overrides.apply(&mut config);
    let validated = validate_config(&config, dry_run)?;
    Ok(ParsedConfig {
        config,
        validated,
        dry_run,
        save_config,
    })
}

pub fn reload_config(
    dry_run: bool,
) -> Result<Option<(Config, ValidatedConfig)>, Box<dyn std::error::Error>> {
    let Some(mut config) = load_config_file()? else {
        return Ok(None);
    };
    apply_env_overrides(&mut config);
    let validated = validate_config(&config, dry_run)?;
    Ok(Some((config, validated)))
}

pub fn config_modified_time() -> Result<Option<SystemTime>, Box<dyn std::error::Error>> {
    let path = config_path()?;
    if !path.exists() {
        return Ok(None);
    }
    Ok(Some(std::fs::metadata(path)?.modified()?))
}

fn print_help() {
    println!("Usage: crypto_tracker [OPTIONS]");
    println!("  --rolling-window 30d --candle-interval 1h");
    println!("  --sideways-band-pct 2 --min-sideways-closes-pct 95");
    println!("  --positive-spike-pct 5 --post-spike-quiet-period 3d");
    println!("  --min-quote-volume-24h 100000000 --max-quote-volume-24h 1000000000");
    println!("  --symbol-refresh-secs 900");
    println!("  --telegram-token TOKEN --telegram-chat-id ID");
    println!("  --healthcheck-url URL --dry-run --save-config");
    println!(
        "Config: ~/.crypto_tracker/config.json; Telegram credentials can use environment variables."
    );
}

pub fn save_config_file(config: &Config) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let path = config_path()?;
    if let Some(dir) = path.parent() {
        std::fs::create_dir_all(dir)?;
    }
    std::fs::write(&path, serde_json::to_string_pretty(config)?)?;
    Ok(path)
}

fn apply_env_overrides(config: &mut Config) {
    if let Ok(token) = std::env::var("TELEGRAM_BOT_TOKEN")
        && !token.is_empty()
    {
        config.telegram_token = token;
    }
    if let Ok(chat_id) = std::env::var("TELEGRAM_CHAT_ID")
        && !chat_id.is_empty()
    {
        config.telegram_chat_id = chat_id;
    }
}

fn config_path() -> Result<PathBuf, Box<dyn std::error::Error>> {
    Ok(PathBuf::from(std::env::var("HOME")?).join(".crypto_tracker/config.json"))
}

fn load_config_file() -> Result<Option<Config>, Box<dyn std::error::Error>> {
    let path = config_path()?;
    if !path.exists() {
        return Ok(None);
    }
    let content = std::fs::read_to_string(&path)?;
    serde_json::from_str(&content).map(Some).map_err(|error| {
        format!("invalid config at {}: {error}. Migrate legacy momentum/listing fields to the sideways scanner schema", path.display()).into()
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_produce_720_hourly_candles_and_72_quiet_candles() {
        let validated = validate_config(&Config::default(), true).unwrap();
        assert_eq!(validated.window_candles, 720);
        assert_eq!(validated.quiet_candles, 72);
    }

    #[test]
    fn rejects_non_divisible_duration() {
        let config = Config {
            rolling_window: "31m".into(),
            candle_interval: "15m".into(),
            ..Config::default()
        };
        assert!(validate_config(&config, true).is_err());
    }

    #[test]
    fn rejects_variable_month_interval() {
        let config = Config {
            candle_interval: "1M".into(),
            ..Config::default()
        };
        assert!(validate_config(&config, true).is_err());
    }

    #[test]
    fn rejects_volume_maximum_below_minimum() {
        let config = Config {
            min_quote_volume_24h: 1_000.0,
            max_quote_volume_24h: 999.0,
            ..Config::default()
        };
        assert!(validate_config(&config, true).is_err());
    }

    #[test]
    fn accepts_http_healthcheck_url() {
        let config = Config {
            healthcheck_url: "https://example.com/healthcheck".into(),
            ..Config::default()
        };
        assert!(validate_config(&config, true).is_ok());
    }

    #[test]
    fn rejects_non_http_healthcheck_url() {
        let config = Config {
            healthcheck_url: "file:///tmp/healthcheck".into(),
            ..Config::default()
        };
        assert!(validate_config(&config, true).is_err());
    }

    #[test]
    fn rejects_legacy_fields() {
        let value = r#"{"lookback_candles":5}"#;
        assert!(serde_json::from_str::<Config>(value).is_err());
    }
}
