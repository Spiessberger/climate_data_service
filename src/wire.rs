use crate::{Diagnostic, DiagnosticKind};
use serde::{Deserialize, Serialize};

const MAX_RECORD_BYTES: usize = 1024;

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct IndoorReading {
    pub(crate) v: u32,
    #[serde(rename = "type")]
    pub(crate) record_type: String,
    pub(crate) boot_id: String,
    pub(crate) seq: u32,
    pub(crate) temperature_celsius: f64,
    pub(crate) relative_humidity_percent: f64,
}

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct WeatherReading {
    pub(crate) v: u32,
    #[serde(rename = "type")]
    pub(crate) record_type: String,
    pub(crate) boot_id: String,
    pub(crate) seq: u32,
    pub(crate) station_id: u8,
    #[serde(deserialize_with = "required_nullable")]
    pub(crate) temperature_celsius: Option<f64>,
    #[serde(deserialize_with = "required_nullable")]
    pub(crate) relative_humidity_percent: Option<u8>,
    #[serde(deserialize_with = "required_nullable")]
    pub(crate) wind_direction_degrees: Option<u16>,
    #[serde(deserialize_with = "required_nullable")]
    pub(crate) wind_speed_mps: Option<f64>,
    #[serde(deserialize_with = "required_nullable")]
    pub(crate) gust_speed_mps: Option<f64>,
    pub(crate) rain_mm: f64,
    #[serde(deserialize_with = "required_nullable")]
    pub(crate) uv_microwatts_per_cm2: Option<u16>,
    #[serde(deserialize_with = "required_nullable")]
    pub(crate) uv_index: Option<u8>,
    #[serde(deserialize_with = "required_nullable")]
    pub(crate) light_lux: Option<f64>,
    pub(crate) battery_low: bool,
    pub(crate) rssi_dbm: f64,
    pub(crate) lqi: u8,
}

// Nullable version-1 fields must still be present; serde's ordinary Option
// handling would silently treat a missing field as null.
fn required_nullable<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::deserialize(deserializer)
}

#[derive(Deserialize)]
struct Envelope {
    v: u32,
    #[serde(rename = "type")]
    record_type: String,
    boot_id: String,
}

pub(crate) enum Record {
    Indoor(IndoorReading),
    Weather(WeatherReading),
    Heartbeat { boot_id: String },
}

impl Record {
    pub(crate) fn boot_id(&self) -> &str {
        match self {
            Self::Indoor(reading) => &reading.boot_id,
            Self::Weather(reading) => &reading.boot_id,
            Self::Heartbeat { boot_id } => boot_id,
        }
    }
}

#[derive(Default)]
pub(crate) struct Framer {
    line: Vec<u8>,
    discarding: Option<DiagnosticKind>,
}

impl Framer {
    pub(crate) fn feed(
        &mut self,
        bytes: &[u8],
        reading: &mut impl FnMut(Record),
        diagnostic: &mut impl FnMut(Diagnostic<'_>),
    ) {
        for &byte in bytes {
            self.line.push(byte);
            if self.discarding.is_some() || (self.line.len() == MAX_RECORD_BYTES && byte != b'\n') {
                let kind = *self.discarding.get_or_insert_with(|| {
                    if self.line.starts_with(b"DATA ") {
                        DiagnosticKind::OversizedLine
                    } else {
                        DiagnosticKind::Text
                    }
                });
                if self.line.len() == MAX_RECORD_BYTES || byte == b'\n' {
                    diagnostic(Diagnostic {
                        kind,
                        bytes: &self.line,
                    });
                    self.line.clear();
                    if byte == b'\n' {
                        self.discarding = None;
                    }
                }
            } else if byte == b'\n' {
                if let Some(json) = self.line.strip_prefix(b"DATA ") {
                    match parse_record(&json[..json.len() - 1]) {
                        Ok(value) => reading(value),
                        Err(_) => diagnostic(Diagnostic {
                            kind: DiagnosticKind::RejectedData,
                            bytes: &self.line,
                        }),
                    }
                } else {
                    diagnostic(Diagnostic {
                        kind: DiagnosticKind::Text,
                        bytes: &self.line,
                    });
                }
                self.line.clear();
            }
        }
    }

    pub(crate) fn finish(&mut self, diagnostic: &mut impl FnMut(Diagnostic<'_>)) {
        if !self.line.is_empty() {
            diagnostic(Diagnostic {
                kind: DiagnosticKind::PartialLine,
                bytes: &self.line,
            });
        }
        self.line.clear();
        self.discarding = None;
    }
}

fn parse_record(json: &[u8]) -> Result<Record, ()> {
    // Validate even ignored extension fields: serde's derived visitor otherwise
    // skips duplicate unknown keys and may skip invalid UTF-8 in ignored strings.
    if json.contains(&b'\r') {
        return Err(());
    }
    let mut deserializer = serde_json::Deserializer::from_slice(json);
    serde::Deserializer::deserialize_map(&mut deserializer, UniqueKeys).map_err(|_| ())?;
    deserializer.end().map_err(|_| ())?;
    let envelope: Envelope = serde_json::from_slice(json).map_err(|_| ())?;
    if envelope.v != 1
        || envelope.boot_id.len() != 32
        || !envelope
            .boot_id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
    {
        return Err(());
    }
    match envelope.record_type.as_str() {
        "heartbeat" => Ok(Record::Heartbeat {
            boot_id: envelope.boot_id,
        }),
        "indoor" => {
            let reading: IndoorReading = serde_json::from_slice(json).map_err(|_| ())?;
            if !reading.temperature_celsius.is_finite()
                || !reading.relative_humidity_percent.is_finite()
            {
                return Err(());
            }
            Ok(Record::Indoor(reading))
        }
        "weather" => {
            let reading: WeatherReading = serde_json::from_slice(json).map_err(|_| ())?;
            if [
                reading.temperature_celsius,
                reading.wind_speed_mps,
                reading.gust_speed_mps,
                reading.light_lux,
                Some(reading.rain_mm),
                Some(reading.rssi_dbm),
            ]
            .into_iter()
            .flatten()
            .any(|value| !value.is_finite())
            {
                return Err(());
            }
            Ok(Record::Weather(reading))
        }
        _ => Err(()),
    }
}

/// Traverse the whole JSON document without retaining values. The framing limit
/// bounds key storage, and serde_json's default recursion limit bounds nesting.
struct UniqueKeys;

impl<'de> Deserialize<'de> for UniqueKeys {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        deserializer.deserialize_any(Self)
    }
}

impl<'de> serde::de::Visitor<'de> for UniqueKeys {
    type Value = Self;

    fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str("JSON without duplicate object keys")
    }
    fn visit_bool<E: serde::de::Error>(self, _: bool) -> Result<Self, E> {
        Ok(self)
    }
    fn visit_i64<E: serde::de::Error>(self, _: i64) -> Result<Self, E> {
        Ok(self)
    }
    fn visit_u64<E: serde::de::Error>(self, _: u64) -> Result<Self, E> {
        Ok(self)
    }
    fn visit_f64<E: serde::de::Error>(self, _: f64) -> Result<Self, E> {
        Ok(self)
    }
    fn visit_str<E: serde::de::Error>(self, _: &str) -> Result<Self, E> {
        Ok(self)
    }
    fn visit_unit<E: serde::de::Error>(self) -> Result<Self, E> {
        Ok(self)
    }

    fn visit_seq<A: serde::de::SeqAccess<'de>>(self, mut sequence: A) -> Result<Self, A::Error> {
        while sequence.next_element::<Self>()?.is_some() {}
        Ok(self)
    }

    fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<Self, A::Error> {
        let mut keys = std::collections::BTreeSet::new();
        while let Some(key) = map.next_key::<String>()? {
            if !keys.insert(key) {
                return Err(serde::de::Error::custom("duplicate object key"));
            }
            map.next_value::<Self>()?;
        }
        Ok(self)
    }
}
