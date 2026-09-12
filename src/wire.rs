use crate::{Diagnostic, DiagnosticKind};
use serde::{Deserialize, Serialize};

const MAX_RECORD_BYTES: usize = 1024;

#[derive(Clone, Deserialize, Serialize)]
pub(crate) struct IndoorReading {
    v: u32,
    #[serde(rename = "type")]
    record_type: String,
    boot_id: String,
    seq: u32,
    temperature_celsius: f64,
    relative_humidity_percent: f64,
}

#[derive(Default)]
pub(crate) struct Framer {
    line: Vec<u8>,
    discarding: bool,
}

impl Framer {
    pub(crate) fn feed(
        &mut self,
        bytes: &[u8],
        reading: &mut impl FnMut(IndoorReading),
        diagnostic: &mut impl FnMut(Diagnostic<'_>),
    ) {
        for &byte in bytes {
            self.line.push(byte);
            if self.discarding || (self.line.len() == MAX_RECORD_BYTES && byte != b'\n') {
                if self.line.len() == MAX_RECORD_BYTES || byte == b'\n' {
                    diagnostic(Diagnostic {
                        kind: DiagnosticKind::OversizedLine,
                        bytes: &self.line,
                    });
                    self.line.clear();
                    self.discarding = byte != b'\n';
                }
            } else if byte == b'\n' {
                if let Some(json) = self.line.strip_prefix(b"DATA ") {
                    match parse_indoor(&json[..json.len() - 1]) {
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
        self.discarding = false;
    }
}

fn parse_indoor(json: &[u8]) -> Result<IndoorReading, ()> {
    // Validate even ignored extension fields: serde's derived visitor otherwise
    // skips duplicate unknown keys and may skip invalid UTF-8 in ignored strings.
    if json.contains(&b'\r') {
        return Err(());
    }
    let mut deserializer = serde_json::Deserializer::from_slice(json);
    serde::Deserializer::deserialize_map(&mut deserializer, UniqueKeys).map_err(|_| ())?;
    deserializer.end().map_err(|_| ())?;
    let reading: IndoorReading = serde_json::from_slice(json).map_err(|_| ())?;
    if reading.v != 1
        || reading.record_type != "indoor"
        || reading.boot_id.len() != 32
        || !reading
            .boot_id
            .bytes()
            .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        || !reading.temperature_celsius.is_finite()
        || !reading.relative_humidity_percent.is_finite()
    {
        return Err(());
    }
    Ok(reading)
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
