use serde::Serialize;
use std::time::Duration;

const TRAFFIC_TIMEOUT: Duration = Duration::from_secs(15);

#[derive(Serialize)]
#[serde(tag = "event", rename_all = "snake_case")]
pub(crate) enum Event {
    SerialConnected {
        path: String,
    },
    SerialUnavailable {
        path: String,
        error: String,
    },
    SerialDisconnected {
        path: String,
        error: String,
    },
    GatewayAvailable,
    GatewayTimeout,
    GatewayRestart {
        previous_boot_id: String,
        boot_id: String,
    },
    ReadingGap {
        stream: &'static str,
        boot_id: String,
        previous_seq: u32,
        seq: u32,
        observed_missing: u32,
    },
    ReadingSequenceAmbiguous {
        stream: &'static str,
        boot_id: String,
        seq: u32,
    },
}

/// Per-stream continuity in the current boot, with a lifetime observed loss total.
/// A first observation cannot establish losses; neither can a repeated counter.
#[derive(Clone, Default, Serialize)]
pub(crate) struct StreamContinuity {
    last_seq: Option<u32>,
    observed_missing_readings: u64,
}

impl StreamContinuity {
    fn observe(&mut self, stream: &'static str, boot_id: &str, seq: u32) -> Option<Event> {
        let previous_seq = self.last_seq.replace(seq)?;
        match seq.wrapping_sub(previous_seq) {
            0 => Some(Event::ReadingSequenceAmbiguous {
                stream,
                boot_id: boot_id.to_owned(),
                seq,
            }),
            1 => None,
            distance => {
                let observed_missing = distance - 1;
                self.observed_missing_readings = self
                    .observed_missing_readings
                    .saturating_add(u64::from(observed_missing));
                Some(Event::ReadingGap {
                    stream,
                    boot_id: boot_id.to_owned(),
                    previous_seq,
                    seq,
                    observed_missing,
                })
            }
        }
    }
}

#[derive(Clone, Default, Serialize)]
pub(crate) struct Gateway {
    pub(crate) available: bool,
    pub(crate) boot_id: Option<String>,
    pub(crate) last_received_at_unix_ms: Option<i64>,
    restart_count: u64,
    indoor: StreamContinuity,
    #[serde(skip)]
    last_traffic: Option<Duration>,
}

impl Gateway {
    pub(crate) fn disconnect(&mut self) {
        self.available = false;
    }

    pub(crate) fn receive(&mut self, boot_id: &str, now: Duration, utc_ms: i64) -> Vec<Event> {
        let mut events = Vec::new();
        if let Some(previous_boot_id) = &self.boot_id
            && previous_boot_id != boot_id
        {
            events.push(Event::GatewayRestart {
                previous_boot_id: previous_boot_id.clone(),
                boot_id: boot_id.to_owned(),
            });
            self.restart_count = self.restart_count.saturating_add(1);
            self.indoor.last_seq = None;
        }
        self.boot_id = Some(boot_id.to_owned());
        self.last_traffic = Some(now);
        self.last_received_at_unix_ms = Some(utc_ms);
        let recovered = !self.available;
        self.available = true;
        if recovered {
            events.push(Event::GatewayAvailable);
        }
        events
    }

    pub(crate) fn indoor(&mut self, seq: u32) -> Option<Event> {
        self.indoor.observe(
            "indoor",
            self.boot_id
                .as_deref()
                .expect("validated record establishes boot"),
            seq,
        )
    }

    pub(crate) fn expire(&mut self, now: Duration) -> Option<Event> {
        if self.available
            && self
                .last_traffic
                .is_some_and(|last| now.saturating_sub(last) >= TRAFFIC_TIMEOUT)
        {
            self.available = false;
            Some(Event::GatewayTimeout)
        } else {
            None
        }
    }
}
