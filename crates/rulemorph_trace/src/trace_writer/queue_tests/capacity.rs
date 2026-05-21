#[test]
fn queue_full_normal_drops_new_trace() {
    let temp_dir = std::env::temp_dir().join("rulemorph-trace-queue-normal-drop");
    let writer = TraceWriter::with_config(
        temp_dir,
        TraceWriterConfig {
            queue_capacity: 1,
            queue_max_bytes: DEFAULT_TRACE_QUEUE_MAX_BYTES,
            write_options: TraceWriteOptions::default(),
            spawn_worker: false,
            write_backend: None,
        },
    );

    assert!(writer.enqueue(make_trace("trace-a", "ok")));
    assert!(!writer.enqueue(make_trace("trace-b", "ok")));

    let guard = writer.queue.items.lock().expect("queue lock");
    assert_eq!(guard.len(), 1);
    assert_eq!(trace_id_for_log(&guard[0].trace), "trace-a");
    assert_eq!(guard[0].priority, TracePriority::Normal);
}

#[test]
fn queue_full_high_evicts_normal_and_keeps_full() {
    let temp_dir = std::env::temp_dir().join("rulemorph-trace-queue-high-evict");
    let writer = TraceWriter::with_config(
        temp_dir,
        TraceWriterConfig {
            queue_capacity: 1,
            queue_max_bytes: DEFAULT_TRACE_QUEUE_MAX_BYTES,
            write_options: TraceWriteOptions::default(),
            spawn_worker: false,
            write_backend: None,
        },
    );

    assert!(writer.enqueue(make_trace("trace-normal", "ok")));

    let options = TraceWriteOptions {
        detail_level: TraceDetailLevel::Full,
        ..Default::default()
    };
    assert!(writer.enqueue_with_options(make_trace("trace-error", "error"), Some(options)));

    let guard = writer.queue.items.lock().expect("queue lock");
    assert_eq!(guard.len(), 1);
    assert_eq!(trace_id_for_log(&guard[0].trace), "trace-error");
    assert_eq!(guard[0].priority, TracePriority::High);
    assert_eq!(guard[0].options.detail_level, TraceDetailLevel::Full);
    assert!(!guard[0].downgraded);
}
