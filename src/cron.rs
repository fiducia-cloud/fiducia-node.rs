//! Standard 5-field cron parsing + next-fire computation, in UTC.
//!
//! Fields, in order: `minute hour day-of-month month day-of-week`. Each field
//! supports `*`, a single value `a`, a range `a-b`, a list `a,b,c`, and steps
//! `*/n` or `a-b/n`. Months accept `JAN..DEC` and days-of-week `SUN..SAT`
//! (case-insensitive); day-of-week accepts both `0` and `7` for Sunday.
//!
//! Day-of-month and day-of-week follow cron's OR rule: when **both** are
//! restricted (neither is `*`), a day matches if *either* matches; when only one
//! is restricted, only that one constrains.
//!
//! [`CronSchedule::next_after`] returns the next firing time strictly after a
//! given instant, computed with civil-date math (no external date crate) by a
//! bounded minute-by-minute search — called only on upsert and after each fire,
//! never on the hot path.

/// A parsed cron expression.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CronSchedule {
    minute: [bool; 60],
    hour: [bool; 24],
    dom: [bool; 32], // index 1..=31
    month: [bool; 13], // index 1..=12
    dow: [bool; 7], // 0=Sun..6=Sat
    dom_restricted: bool,
    dow_restricted: bool,
}

/// Search horizon: if nothing matches within this many years, the expression
/// fires never (e.g. an impossible day/month combination).
const MAX_SEARCH_MINUTES: u64 = 5 * 366 * 24 * 60;

impl CronSchedule {
    /// Parse a standard 5-field cron expression.
    pub fn parse(expr: &str) -> Result<CronSchedule, String> {
        let fields: Vec<&str> = expr.split_whitespace().collect();
        if fields.len() != 5 {
            return Err(format!("expected 5 fields, got {}", fields.len()));
        }
        let (minute, _) = parse_field(fields[0], 0, 59, &[])?;
        let (hour, _) = parse_field(fields[1], 0, 23, &[])?;
        let (dom_vec, dom_restricted) = parse_field(fields[2], 1, 31, &[])?;
        let (month, _) = parse_field(fields[3], 1, 12, MONTHS)?;
        let (dow_raw, dow_restricted) = parse_field(fields[4], 0, 7, DAYS)?;

        let mut minute_a = [false; 60];
        for v in minute {
            minute_a[v as usize] = true;
        }
        let mut hour_a = [false; 24];
        for v in hour {
            hour_a[v as usize] = true;
        }
        let mut dom_a = [false; 32];
        for v in dom_vec {
            dom_a[v as usize] = true;
        }
        let mut month_a = [false; 13];
        for v in month {
            month_a[v as usize] = true;
        }
        // Day-of-week: fold 7 (Sunday) onto 0 so both spellings match.
        let mut dow_a = [false; 7];
        for v in dow_raw {
            dow_a[(v % 7) as usize] = true;
        }

        Ok(CronSchedule {
            minute: minute_a,
            hour: hour_a,
            dom: dom_a,
            month: month_a,
            dow: dow_a,
            dom_restricted,
            dow_restricted,
        })
    }

    fn matches(&self, parts: &DateParts) -> bool {
        if !self.minute[parts.minute as usize]
            || !self.hour[parts.hour as usize]
            || !self.month[parts.month as usize]
        {
            return false;
        }
        let dom_ok = self.dom[parts.day as usize];
        let dow_ok = self.dow[parts.dow as usize];
        match (self.dom_restricted, self.dow_restricted) {
            (false, false) => true,
            (true, false) => dom_ok,
            (false, true) => dow_ok,
            (true, true) => dom_ok || dow_ok,
        }
    }

    /// The next firing time (epoch ms, UTC) strictly after `after_ms`, or `None`
    /// if the expression won't fire within the search horizon.
    pub fn next_after(&self, after_ms: u64) -> Option<u64> {
        // Start at the first whole minute strictly after `after_ms`.
        let start_min = after_ms / 60_000 + 1;
        for offset in 0..MAX_SEARCH_MINUTES {
            let minute = start_min + offset;
            let parts = DateParts::from_epoch_minute(minute);
            if self.matches(&parts) {
                return Some(minute * 60_000);
            }
        }
        None
    }
}

const MONTHS: &[(&str, u32)] = &[
    ("jan", 1), ("feb", 2), ("mar", 3), ("apr", 4), ("may", 5), ("jun", 6),
    ("jul", 7), ("aug", 8), ("sep", 9), ("oct", 10), ("nov", 11), ("dec", 12),
];
const DAYS: &[(&str, u32)] = &[
    ("sun", 0), ("mon", 1), ("tue", 2), ("wed", 3), ("thu", 4), ("fri", 5), ("sat", 6),
];

/// Parse one field into the set of values it matches, plus whether it is
/// *restricted* (anything other than a bare `*`). `max` is inclusive.
fn parse_field(spec: &str, min: u32, max: u32, names: &[(&str, u32)]) -> Result<(Vec<u32>, bool), String> {
    let mut values = Vec::new();
    let restricted = spec != "*";
    for part in spec.split(',') {
        let (range_spec, step) = match part.split_once('/') {
            Some((r, s)) => {
                let step: u32 = s.parse().map_err(|_| format!("bad step '{s}'"))?;
                if step == 0 {
                    return Err("step must be >= 1".to_string());
                }
                (r, step)
            }
            None => (part, 1),
        };
        let (lo, hi) = if range_spec == "*" {
            (min, max)
        } else if let Some((a, b)) = range_spec.split_once('-') {
            (parse_value(a, names)?, parse_value(b, names)?)
        } else {
            let v = parse_value(range_spec, names)?;
            // A bare value with a step (`a/n`) ranges from `a` to the field max.
            if step > 1 {
                (v, max)
            } else {
                (v, v)
            }
        };
        if lo < min || hi > max || lo > hi {
            return Err(format!("value out of range [{min},{max}] in '{part}'"));
        }
        let mut v = lo;
        while v <= hi {
            values.push(v);
            v += step;
        }
    }
    Ok((values, restricted))
}

fn parse_value(token: &str, names: &[(&str, u32)]) -> Result<u32, String> {
    let token = token.trim();
    if let Ok(n) = token.parse::<u32>() {
        return Ok(n);
    }
    let lower = token.to_ascii_lowercase();
    names
        .iter()
        .find(|(name, _)| *name == lower)
        .map(|(_, v)| *v)
        .ok_or_else(|| format!("unknown value '{token}'"))
}

/// UTC calendar parts of an instant, for cron matching.
struct DateParts {
    minute: u32,
    hour: u32,
    day: u32,   // 1..=31
    month: u32, // 1..=12
    dow: u32,   // 0=Sun..6=Sat
}

impl DateParts {
    fn from_epoch_minute(total_min: u64) -> DateParts {
        let days = (total_min / 1440) as i64;
        let mins_in_day = (total_min % 1440) as u32;
        let (_year, month, day) = civil_from_days(days);
        // 1970-01-01 was a Thursday; offset so 0 = Sunday.
        let dow = (((days % 7 + 7) % 7) as u32 + 4) % 7;
        DateParts {
            minute: mins_in_day % 60,
            hour: mins_in_day / 60,
            day,
            month,
            dow,
        }
    }
}

/// Civil date from a count of days since 1970-01-01 (proleptic Gregorian).
/// Howard Hinnant's algorithm.
fn civil_from_days(z: i64) -> (i64, u32, u32) {
    let z = z + 719_468;
    let era = (if z >= 0 { z } else { z - 146_096 }) / 146_097;
    let doe = z - era * 146_097; // [0, 146096]
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365; // [0, 399]
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100); // [0, 365]
    let mp = (5 * doy + 2) / 153; // [0, 11]
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32; // [1, 31]
    let m = (if mp < 10 { mp + 3 } else { mp - 9 }) as u32; // [1, 12]
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIN: u64 = 60_000;
    const DAY: u64 = 1440 * MIN;

    #[test]
    fn rejects_malformed_expressions() {
        assert!(CronSchedule::parse("* * * *").is_err(), "too few fields");
        assert!(CronSchedule::parse("* * * * * *").is_err(), "too many fields");
        assert!(CronSchedule::parse("60 * * * *").is_err(), "minute out of range");
        assert!(CronSchedule::parse("* 24 * * *").is_err(), "hour out of range");
        assert!(CronSchedule::parse("*/0 * * * *").is_err(), "zero step");
        assert!(CronSchedule::parse("5-1 * * * *").is_err(), "reversed range");
        assert!(CronSchedule::parse("* * * FOO *").is_err(), "bad month name");
    }

    #[test]
    fn accepts_standard_forms() {
        for expr in [
            "* * * * *",
            "*/15 * * * *",
            "0 9 * * 1-5",
            "0 0 1 * *",
            "0 0 * * 0",
            "0 0 * * 7",
            "30 8,12,17 * * MON-FRI",
            "0 0 1 JAN *",
        ] {
            assert!(CronSchedule::parse(expr).is_ok(), "should parse: {expr}");
        }
    }

    #[test]
    fn every_15_minutes_steps_by_quarter_hour() {
        let c = CronSchedule::parse("*/15 * * * *").unwrap();
        // Epoch 0 is itself a fire point; next *strictly after* is +15m.
        assert_eq!(c.next_after(0), Some(15 * MIN));
        assert_eq!(c.next_after(15 * MIN), Some(30 * MIN));
        assert_eq!(c.next_after(31 * MIN), Some(45 * MIN));
    }

    #[test]
    fn daily_midnight_advances_one_day() {
        let c = CronSchedule::parse("0 0 * * *").unwrap();
        // 1970-01-01T00:00Z is a fire; next strictly after is the following midnight.
        assert_eq!(c.next_after(0), Some(DAY));
        assert_eq!(c.next_after(DAY / 2), Some(DAY));
    }

    #[test]
    fn day_of_week_matches_calendar() {
        // 1970-01-01 is a Thursday(4); 1970-01-02 is a Friday(5).
        let fri = CronSchedule::parse("0 0 * * 5").unwrap();
        assert_eq!(fri.next_after(0), Some(DAY), "next Friday is day 1");
        let thu = CronSchedule::parse("0 0 * * 4").unwrap();
        assert_eq!(thu.next_after(0), Some(7 * DAY), "next Thursday after day 0 is day 7");
        // Sunday via both spellings resolves identically.
        assert_eq!(
            CronSchedule::parse("0 0 * * 0").unwrap().next_after(0),
            CronSchedule::parse("0 0 * * 7").unwrap().next_after(0),
        );
    }

    #[test]
    fn weekday_business_hours_skips_the_weekend() {
        // 09:00 Mon-Fri. From Fri 1970-01-02 18:00, next is Mon 1970-01-05 09:00.
        let c = CronSchedule::parse("0 9 * * 1-5").unwrap();
        let fri_evening = DAY + 18 * 60 * MIN; // day1 18:00
        let mon_morning = 4 * DAY + 9 * 60 * MIN; // day4 (Mon) 09:00
        assert_eq!(c.next_after(fri_evening), Some(mon_morning));
    }

    #[test]
    fn dom_and_dow_both_restricted_is_an_or() {
        // Fire on the 1st of the month OR on any Monday.
        let c = CronSchedule::parse("0 0 1 * 1").unwrap();
        // day0=Thu Jan 1. next strictly after day0 midnight: first of {next Monday,
        // next 1st-of-month}. Next Monday = day4 (Jan 5). Next 1st = Feb 1 (day31).
        assert_eq!(c.next_after(0), Some(4 * DAY));
    }

    #[test]
    fn civil_from_days_matches_known_dates() {
        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(31), (1970, 2, 1));
        assert_eq!(civil_from_days(365), (1971, 1, 1));
        // 2000 was a leap year: day 59 of 2000 is Feb 29.
        let y2k = days_from_civil(2000, 1, 1);
        assert_eq!(civil_from_days(y2k + 59), (2000, 2, 29));
    }

    // Inverse of civil_from_days, used only by the test above.
    fn days_from_civil(y: i64, m: u32, d: u32) -> i64 {
        let y = if m <= 2 { y - 1 } else { y };
        let era = (if y >= 0 { y } else { y - 399 }) / 400;
        let yoe = y - era * 400;
        let mp = if m > 2 { m - 3 } else { m + 9 } as i64;
        let doy = (153 * mp + 2) / 5 + d as i64 - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        era * 146_097 + doe - 719_468
    }
}
