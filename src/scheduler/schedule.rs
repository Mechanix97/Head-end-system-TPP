use std::fmt;

use chrono::{NaiveDate, NaiveDateTime, NaiveTime};

pub struct Schedule {
    pub sec: usize,
    pub min: usize,
    pub hour: usize,
    pub day: usize,
    pub mon: usize,
    pub year: usize,
}

impl fmt::Display for Schedule {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{:02}:{:02}:{:02} {}/{}/{}",
            self.hour, self.min, self.sec, self.day, self.mon, self.year
        )
    }
}

impl TryFrom<&Schedule> for NaiveDateTime {
    type Error = String;

    fn try_from(s: &Schedule) -> Result<Self, Self::Error> {
        let date = NaiveDate::from_ymd_opt(s.year as i32, s.mon as u32, s.day as u32)
            .ok_or_else(|| format!("invalid date: {}/{}/{}", s.day, s.mon, s.year))?;
        let time = NaiveTime::from_hms_opt(s.hour as u32, s.min as u32, s.sec as u32)
            .ok_or_else(|| format!("invalid time: {}:{}:{}", s.hour, s.min, s.sec))?;
        Ok(NaiveDateTime::new(date, time))
    }
}
