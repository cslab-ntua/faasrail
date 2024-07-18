use std::str::FromStr;

use super::Error;

#[derive(Debug, Clone, Copy)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub struct MinuteRange(u16, u16);

impl Default for MinuteRange {
    fn default() -> Self {
        Self(1, u16::MAX)
    }
}

impl MinuteRange {
    pub fn new_inclusive(first: u16, last: u16) -> Result<Self, Error> {
        if first == 0 {
            return Err(Error::Minute {
                msg: "minute indexing is 1-based".into(),
                source: None,
            });
        }
        if first > last {
            return Err(Error::Minute {
                msg: format!("{first} == first > last == {last}").into_boxed_str(),
                source: None,
            });
        }
        Ok(Self(first, last))
    }

    #[inline(always)]
    pub fn start(&self) -> u16 {
        self.0
    }

    #[inline(always)]
    pub fn end(&self) -> u16 {
        self.1
    }

    #[inline(always)]
    pub fn contains(&self, minute: u16) -> bool {
        minute >= self.0 && minute <= self.1
    }
}

impl FromStr for MinuteRange {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim();
        s.split_once(':')
            .or_else(|| s.split_once(".."))
            .map_or_else(
                || {
                    Err(Error::Minute {
                        msg: format!("invalid format {s:?}").into_boxed_str(),
                        source: None,
                    })
                },
                |(first, last)| {
                    Self::new_inclusive(
                        first.trim().parse().map_err(|err| Error::Minute {
                            msg: "first".into(),
                            source: Some(err),
                        })?,
                        last.trim().parse().map_err(|err| Error::Minute {
                            msg: "last".into(),
                            source: Some(err),
                        })?,
                    )
                },
            )
    }
}

#[cfg(test)]
mod tests {
    use super::MinuteRange;

    #[test]
    fn minuterange01() {
        let default = MinuteRange::default();
        assert_eq!(default, "1..65535".parse().unwrap());
        assert_eq!(default, "1:65535".parse().unwrap());
    }
}
