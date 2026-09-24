use chrono::{DateTime, Local};

pub fn format_timestamp_short(time: DateTime<Local>) -> String {
    time.format("%d.%m.%Y %H:%M").to_string()
}
