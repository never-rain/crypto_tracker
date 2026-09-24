use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Candle {
    pub open_time_ms: i64,
    pub close_time_ms: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub quote_volume: f64,
}

#[derive(Debug, Clone, PartialEq)]
pub struct CandidateResult {
    pub symbol: String,
    pub median_price: f64,
    pub band_lower: f64,
    pub band_upper: f64,
    pub sideways_close_ratio: f64,
    pub latest_close: f64,
    pub latest_spike_time_ms: i64,
    pub latest_spike_high: f64,
    pub latest_spike_pct: f64,
    pub largest_spike_time_ms: i64,
    pub largest_spike_pct: f64,
    pub quiet_period_ms: i64,
    pub quote_volume_24h: f64,
}

#[derive(Debug, Clone, Copy)]
pub struct DetectorSettings {
    pub window_candles: usize,
    pub interval_ms: i64,
    pub sideways_band_pct: f64,
    pub min_sideways_closes_pct: f64,
    pub positive_spike_pct: f64,
    pub quiet_period_ms: i64,
}

pub fn detect_candidate(
    symbol: &str,
    candles: &[Candle],
    settings: DetectorSettings,
    quote_volume_24h: f64,
) -> Option<CandidateResult> {
    if candles.len() < settings.window_candles {
        return None;
    }
    let window = &candles[candles.len() - settings.window_candles..];
    if !valid_complete_window(window, settings.interval_ms) {
        return None;
    }

    let mut closes: Vec<f64> = window.iter().map(|candle| candle.close).collect();
    if closes
        .iter()
        .any(|price| !price.is_finite() || *price <= 0.0)
    {
        return None;
    }
    closes.sort_by(f64::total_cmp);
    let median_price = median(&closes);
    let band_fraction = settings.sideways_band_pct / 100.0;
    let band_lower = median_price * (1.0 - band_fraction);
    let band_upper = median_price * (1.0 + band_fraction);
    let in_band = window
        .iter()
        .filter(|candle| candle.close >= band_lower && candle.close <= band_upper)
        .count();
    let sideways_close_ratio = in_band as f64 / window.len() as f64 * 100.0;
    if sideways_close_ratio + f64::EPSILON < settings.min_sideways_closes_pct {
        return None;
    }

    let spike_level = median_price * (1.0 + settings.positive_spike_pct / 100.0);
    let spikes: Vec<&Candle> = window
        .iter()
        .filter(|candle| candle.high.is_finite() && candle.high >= spike_level)
        .collect();
    let latest_spike = *spikes.iter().max_by_key(|candle| candle.open_time_ms)?;
    let largest_spike = *spikes
        .iter()
        .max_by(|left, right| left.high.total_cmp(&right.high))?;
    let latest = window.last()?;
    let quiet_period_ms = latest.close_time_ms - latest_spike.close_time_ms;
    if quiet_period_ms < settings.quiet_period_ms {
        return None;
    }
    let quiet_start_ms = latest.close_time_ms - settings.quiet_period_ms;
    if window.iter().any(|candle| {
        candle.close_time_ms > quiet_start_ms
            && (candle.close < band_lower || candle.close > band_upper)
    }) {
        return None;
    }

    Some(CandidateResult {
        symbol: symbol.to_string(),
        median_price,
        band_lower,
        band_upper,
        sideways_close_ratio,
        latest_close: latest.close,
        latest_spike_time_ms: latest_spike.open_time_ms,
        latest_spike_high: latest_spike.high,
        latest_spike_pct: percentage_above(latest_spike.high, median_price),
        largest_spike_time_ms: largest_spike.open_time_ms,
        largest_spike_pct: percentage_above(largest_spike.high, median_price),
        quiet_period_ms,
        quote_volume_24h,
    })
}

fn median(sorted: &[f64]) -> f64 {
    let middle = sorted.len() / 2;
    if sorted.len().is_multiple_of(2) {
        (sorted[middle - 1] + sorted[middle]) / 2.0
    } else {
        sorted[middle]
    }
}

fn percentage_above(value: f64, baseline: f64) -> f64 {
    (value / baseline - 1.0) * 100.0
}

fn valid_complete_window(candles: &[Candle], interval_ms: i64) -> bool {
    candles.iter().all(|candle| {
        candle.open_time_ms >= 0
            && candle.close_time_ms > candle.open_time_ms
            && candle.open.is_finite()
            && candle.high.is_finite()
            && candle.low.is_finite()
            && candle.close.is_finite()
            && candle.open > 0.0
            && candle.high > 0.0
            && candle.low > 0.0
            && candle.close > 0.0
            && candle.high >= candle.open.max(candle.close)
            && candle.low <= candle.open.min(candle.close)
            && candle.high >= candle.low
    }) && candles.windows(2).all(|pair| {
        pair[1].open_time_ms - pair[0].open_time_ms == interval_ms
            && pair[1].open_time_ms > pair[0].open_time_ms
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const HOUR: i64 = 3_600_000;

    fn candle(index: usize, close: f64, high: f64) -> Candle {
        let open_time_ms = index as i64 * HOUR;
        Candle {
            open_time_ms,
            close_time_ms: open_time_ms + HOUR - 1,
            open: close,
            high,
            low: close.min(high) * 0.999,
            close,
            quote_volume: 1_000.0,
        }
    }

    fn settings() -> DetectorSettings {
        DetectorSettings {
            window_candles: 100,
            interval_ms: HOUR,
            sideways_band_pct: 2.0,
            min_sideways_closes_pct: 95.0,
            positive_spike_pct: 5.0,
            quiet_period_ms: 3 * HOUR,
        }
    }

    #[test]
    fn finds_sideways_symbol_with_historical_positive_wick() {
        let mut candles: Vec<_> = (0..100).map(|index| candle(index, 100.0, 100.1)).collect();
        candles[50].high = 106.0;
        let result = detect_candidate("TESTUSDT", &candles, settings(), 123.0).unwrap();
        assert_eq!(result.median_price, 100.0);
        assert_eq!(result.band_lower, 98.0);
        assert_eq!(result.band_upper, 102.0);
        assert!((result.latest_spike_pct - 6.0).abs() < 1e-9);
    }

    #[test]
    fn accepts_exact_spike_boundary() {
        let mut candles: Vec<_> = (0..100).map(|index| candle(index, 100.0, 100.1)).collect();
        candles[50].high = 105.0;
        assert!(detect_candidate("TESTUSDT", &candles, settings(), 0.0).is_some());
    }

    #[test]
    fn rejects_high_above_band_but_below_spike_level() {
        let mut candles: Vec<_> = (0..100).map(|index| candle(index, 100.0, 100.1)).collect();
        candles[50].high = 102.1;
        assert!(detect_candidate("TESTUSDT", &candles, settings(), 0.0).is_none());
    }

    #[test]
    fn rejects_sideways_window_without_spike() {
        let candles: Vec<_> = (0..100).map(|index| candle(index, 100.0, 101.0)).collect();
        assert!(detect_candidate("TESTUSDT", &candles, settings(), 0.0).is_none());
    }

    #[test]
    fn accepts_closes_on_band_boundaries() {
        let mut candles: Vec<_> = (0..100).map(|index| candle(index, 100.0, 100.1)).collect();
        candles[0] = candle(0, 98.0, 98.1);
        candles[1] = candle(1, 102.0, 102.1);
        candles[50].high = 105.0;
        assert!(detect_candidate("TESTUSDT", &candles, settings(), 0.0).is_some());
    }

    #[test]
    fn newer_spike_restarts_quiet_period() {
        let mut candles: Vec<_> = (0..100).map(|index| candle(index, 100.0, 100.1)).collect();
        candles[50].high = 110.0;
        candles[98].high = 105.0;
        assert!(detect_candidate("TESTUSDT", &candles, settings(), 0.0).is_none());
    }

    #[test]
    fn rejects_more_than_five_percent_closes_outside_band() {
        let mut candles: Vec<_> = (0..100).map(|index| candle(index, 100.0, 100.1)).collect();
        candles[40].high = 106.0;
        for value in candles.iter_mut().take(6) {
            value.close = 110.0;
            value.open = 110.0;
            value.high = 110.1;
            value.low = 109.9;
        }
        assert!(detect_candidate("TESTUSDT", &candles, settings(), 0.0).is_none());
    }

    #[test]
    fn rejects_spike_without_completed_quiet_period() {
        let mut candles: Vec<_> = (0..100).map(|index| candle(index, 100.0, 100.1)).collect();
        candles[98].high = 106.0;
        assert!(detect_candidate("TESTUSDT", &candles, settings(), 0.0).is_none());
    }

    #[test]
    fn rejects_close_outside_band_during_quiet_period() {
        let mut candles: Vec<_> = (0..100).map(|index| candle(index, 100.0, 100.1)).collect();
        candles[50].high = 106.0;
        candles[98] = candle(98, 103.0, 103.1);
        assert!(detect_candidate("TESTUSDT", &candles, settings(), 0.0).is_none());
    }

    #[test]
    fn rejects_negative_outlier_without_positive_spike() {
        let mut candles: Vec<_> = (0..100).map(|index| candle(index, 100.0, 100.1)).collect();
        candles[50] = candle(50, 94.0, 94.1);
        assert!(detect_candidate("TESTUSDT", &candles, settings(), 0.0).is_none());
    }

    #[test]
    fn rejects_incomplete_or_gapped_window() {
        let mut candles: Vec<_> = (0..100).map(|index| candle(index, 100.0, 100.1)).collect();
        candles[50].high = 106.0;
        candles[70].open_time_ms += 1;
        assert!(detect_candidate("TESTUSDT", &candles, settings(), 0.0).is_none());
        assert!(detect_candidate("TESTUSDT", &candles[..99], settings(), 0.0).is_none());
    }
}
