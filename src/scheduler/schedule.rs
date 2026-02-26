use std::fmt;

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
