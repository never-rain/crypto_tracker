use crate::detector::CandidateResult;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

#[derive(Debug, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct AlertState {
    alerted_spikes: HashMap<String, i64>,
}

impl AlertState {
    pub fn load() -> Result<Self, Box<dyn std::error::Error>> {
        let path = state_path()?;
        Self::load_from(&path)
    }

    fn load_from(path: &Path) -> Result<Self, Box<dyn std::error::Error>> {
        if !path.exists() {
            return Ok(Self::default());
        }
        Ok(serde_json::from_str(&std::fs::read_to_string(path)?)?)
    }

    pub fn is_alerted(&self, candidate: &CandidateResult, interval: &str) -> bool {
        self.alerted_spikes
            .contains_key(&alert_key(candidate, interval))
    }

    pub fn mark_alerted(&mut self, candidate: &CandidateResult, interval: &str) {
        self.alerted_spikes.insert(
            alert_key(candidate, interval),
            candidate.latest_spike_time_ms,
        );
    }

    pub fn prune(&mut self, oldest_allowed_ms: i64) {
        self.alerted_spikes
            .retain(|_, spike_time_ms| *spike_time_ms >= oldest_allowed_ms);
    }

    pub fn save(&self) -> Result<PathBuf, Box<dyn std::error::Error>> {
        let path = state_path()?;
        self.save_to(&path)?;
        Ok(path)
    }

    fn save_to(&self, path: &Path) -> Result<(), Box<dyn std::error::Error>> {
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let temp_path = temporary_path(path);
        std::fs::write(&temp_path, serde_json::to_vec_pretty(self)?)?;
        std::fs::rename(&temp_path, path)?;
        Ok(())
    }
}

fn alert_key(candidate: &CandidateResult, interval: &str) -> String {
    format!(
        "{}|{}|{}",
        candidate.symbol, interval, candidate.latest_spike_time_ms
    )
}

fn state_path() -> Result<PathBuf, Box<dyn std::error::Error>> {
    Ok(PathBuf::from(std::env::var("HOME")?).join(".crypto_tracker/state.json"))
}

fn temporary_path(path: &Path) -> PathBuf {
    path.with_extension("json.tmp")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn candidate(timestamp: i64) -> CandidateResult {
        CandidateResult {
            symbol: "BTCUSDT".into(),
            median_price: 1.0,
            band_lower: 0.98,
            band_upper: 1.02,
            sideways_close_ratio: 95.0,
            latest_close: 1.0,
            latest_spike_time_ms: timestamp,
            latest_spike_high: 1.1,
            latest_spike_pct: 10.0,
            largest_spike_time_ms: timestamp,
            largest_spike_pct: 10.0,
            quiet_period_ms: 1,
            quote_volume_24h: 1.0,
        }
    }

    #[test]
    fn deduplicates_by_symbol_interval_and_spike() {
        let mut state = AlertState::default();
        let first = candidate(100);
        assert!(!state.is_alerted(&first, "1h"));
        state.mark_alerted(&first, "1h");
        assert!(state.is_alerted(&first, "1h"));
        assert!(!state.is_alerted(&candidate(200), "1h"));
        assert!(!state.is_alerted(&first, "4h"));
    }

    #[test]
    fn prunes_expired_spikes() {
        let mut state = AlertState::default();
        state.mark_alerted(&candidate(100), "1h");
        state.mark_alerted(&candidate(200), "1h");
        state.prune(150);
        assert!(!state.is_alerted(&candidate(100), "1h"));
        assert!(state.is_alerted(&candidate(200), "1h"));
    }

    #[test]
    fn persists_deduplication_across_load() {
        let path = std::env::temp_dir().join(format!(
            "crypto_tracker_state_test_{}.json",
            std::process::id()
        ));
        let mut state = AlertState::default();
        state.mark_alerted(&candidate(123), "1h");
        state.save_to(&path).unwrap();

        let restored = AlertState::load_from(&path).unwrap();
        assert!(restored.is_alerted(&candidate(123), "1h"));

        std::fs::remove_file(path).unwrap();
    }
}
