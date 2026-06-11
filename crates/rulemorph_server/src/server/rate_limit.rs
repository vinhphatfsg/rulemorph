use std::collections::HashMap;
use std::time::{Duration, Instant};

use tokio::sync::Mutex;

#[derive(Debug)]
pub struct RateLimiter {
    limit: u64,
    state: Mutex<HashMap<String, RateLimitState>>,
    max_entries: usize,
    ttl: Duration,
}

#[derive(Debug)]
struct RateLimitState {
    window_start: Instant,
    count: u64,
    last_seen: Instant,
}

impl RateLimiter {
    pub fn new(limit: u64) -> Self {
        Self {
            limit,
            state: Mutex::new(HashMap::new()),
            max_entries: 10_000,
            ttl: Duration::from_secs(600),
        }
    }

    pub async fn allow(&self, key: &str) -> bool {
        let mut state = self.state.lock().await;
        let now = Instant::now();
        if state.len() >= self.max_entries {
            state.retain(|_, entry| now.duration_since(entry.last_seen) <= self.ttl);
            if state.len() >= self.max_entries
                && let Some((oldest_key, _)) = state.iter().min_by_key(|(_, entry)| entry.last_seen)
            {
                let oldest_key = oldest_key.clone();
                state.remove(&oldest_key);
            }
        }

        let entry = state.entry(key.to_string()).or_insert(RateLimitState {
            window_start: now,
            count: 0,
            last_seen: now,
        });
        entry.last_seen = now;
        if now.duration_since(entry.window_start) >= Duration::from_secs(1) {
            entry.window_start = now;
            entry.count = 0;
        }
        if entry.count >= self.limit {
            return false;
        }
        entry.count += 1;
        true
    }
}
