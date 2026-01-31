use std::path::PathBuf;
use std::time::{Duration, SystemTime};

use notify::{RecursiveMode, Watcher};
use tokio::sync::broadcast;
use tracing::warn;
use walkdir::WalkDir;

pub fn start_trace_watcher(data_dir: PathBuf, sender: broadcast::Sender<()>) {
    tokio::spawn(async move {
        let traces_dir = data_dir.join("traces");
        if let Err(err) = tokio::fs::create_dir_all(&traces_dir).await {
            warn!("failed to create traces dir: {}", err);
        }

        let (event_tx, mut event_rx) = tokio::sync::mpsc::unbounded_channel();
        let mut watcher = match notify::recommended_watcher(move |res| {
            let _ = event_tx.send(res);
        }) {
            Ok(watcher) => Some(watcher),
            Err(err) => {
                warn!("trace watcher disabled: {}", err);
                None
            }
        };

        if let Some(watcher_ref) = watcher.as_mut() {
            if let Err(err) = watcher_ref.watch(&traces_dir, RecursiveMode::Recursive) {
                warn!("trace watcher disabled: {}", err);
            }
        }

        let mut last_mtime = latest_mtime(&traces_dir).await;
        let mut interval = tokio::time::interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                _ = interval.tick() => {
                    let latest = latest_mtime(&traces_dir).await;
                    if is_newer(latest, last_mtime) {
                        last_mtime = latest;
                        let _ = sender.send(());
                    }
                }
                Some(_event) = event_rx.recv() => {
                    let _ = sender.send(());
                }
            }
        }
    });
}

fn is_newer(current: Option<SystemTime>, previous: Option<SystemTime>) -> bool {
    match (current, previous) {
        (Some(current), Some(previous)) => current > previous,
        (Some(_), None) => true,
        _ => false,
    }
}

async fn latest_mtime(dir: &PathBuf) -> Option<SystemTime> {
    let dir = dir.clone();
    tokio::task::spawn_blocking(move || {
        let mut latest: Option<SystemTime> = None;
        for entry in WalkDir::new(&dir).into_iter().filter_map(|e| e.ok()) {
            if !entry.file_type().is_file() {
                continue;
            }
            if entry.path().extension().and_then(|s| s.to_str()) != Some("json") {
                continue;
            }
            let modified = entry.metadata().ok().and_then(|meta| meta.modified().ok());
            if let Some(modified) = modified {
                latest = Some(match latest {
                    Some(prev) if prev > modified => prev,
                    _ => modified,
                });
            }
        }
        latest
    })
    .await
    .ok()
    .flatten()
}
