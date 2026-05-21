use serde_json::Value as JsonValue;

use super::TraceCollector;
use crate::trace::schema::{TraceEvent, TraceTruncation};
use crate::trace::snapshot::value_size_bytes;

impl TraceCollector {
    pub(crate) fn push(&mut self, event: TraceEvent) -> bool {
        if self.frozen {
            return false;
        }
        let snapshot_truncated = event.output.as_ref().is_some_and(|snapshot| {
            snapshot.redaction_reason.as_deref() == Some("max_snapshot_bytes")
        }) || event
            .inputs
            .iter()
            .any(|snapshot| snapshot.redaction_reason.as_deref() == Some("max_snapshot_bytes"));
        if let Some(max_events) = self.options.max_events {
            let emitted = self.emitted_events();
            if emitted >= max_events {
                self.complete = false;
                self.truncation = Some(TraceTruncation {
                    reason: "max_events".to_string(),
                    emitted_events: emitted,
                    emitted_bytes: self.emitted_bytes,
                });
                self.frozen = true;
                self.structural_truncated = true;
                return false;
            }
        }
        let event_bytes =
            value_size_bytes(&serde_json::to_value(&event).unwrap_or(JsonValue::Null));
        if let Some(max_trace_bytes) = self.options.max_trace_bytes
            && self.emitted_bytes.saturating_add(event_bytes) > max_trace_bytes
        {
            self.complete = false;
            self.truncation = Some(TraceTruncation {
                reason: "max_trace_bytes".to_string(),
                emitted_events: self.emitted_events(),
                emitted_bytes: self.emitted_bytes,
            });
            self.frozen = true;
            self.structural_truncated = true;
            return false;
        }
        self.emitted_bytes = self.emitted_bytes.saturating_add(event_bytes);
        if snapshot_truncated {
            self.complete = false;
            let emitted_events = self.emitted_events();
            self.truncation.get_or_insert_with(|| TraceTruncation {
                reason: "max_snapshot_bytes".to_string(),
                emitted_events,
                emitted_bytes: self.emitted_bytes,
            });
        }
        self.contains_raw_values |= event
            .output
            .as_ref()
            .is_some_and(|snapshot| snapshot.contains_raw_value)
            || event
                .inputs
                .iter()
                .any(|snapshot| snapshot.contains_raw_value);
        match self.scope {
            super::TraceScope::Record => {
                if let Some(record) = &mut self.current_record {
                    record.events.push(event);
                }
            }
            super::TraceScope::Finalize => {
                self.finalize_events.push(event);
            }
        }
        true
    }

    fn emitted_events(&self) -> usize {
        self.records
            .iter()
            .map(|record| record.events.len())
            .sum::<usize>()
            + self
                .current_record
                .as_ref()
                .map(|record| record.events.len())
                .unwrap_or(0)
            + self.finalize_events.len()
    }
}
