use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::{
    Mutex,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};

use rulemorph::{parse_rule_file, transform, transform_record, transform_stream};
use serde_json::json;

struct CountingAlloc;

static ENABLED: AtomicBool = AtomicBool::new(false);
static ALLOCS: AtomicUsize = AtomicUsize::new(0);
static BYTES: AtomicUsize = AtomicUsize::new(0);
static CURRENT_BYTES: AtomicUsize = AtomicUsize::new(0);
static PEAK_BYTES: AtomicUsize = AtomicUsize::new(0);
static MEASURE_LOCK: Mutex<()> = Mutex::new(());
static TEST_LOCK: Mutex<()> = Mutex::new(());

#[global_allocator]
static GLOBAL: CountingAlloc = CountingAlloc;

unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let ptr = unsafe { System.alloc(layout) };
        if ENABLED.load(Ordering::Relaxed) && !ptr.is_null() {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(layout.size(), Ordering::Relaxed);
            let current = CURRENT_BYTES.fetch_add(layout.size(), Ordering::Relaxed) + layout.size();
            let mut observed = PEAK_BYTES.load(Ordering::Relaxed);
            while current > observed
                && PEAK_BYTES
                    .compare_exchange(observed, current, Ordering::Relaxed, Ordering::Relaxed)
                    .is_err()
            {
                observed = PEAK_BYTES.load(Ordering::Relaxed);
            }
        }
        ptr
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if ENABLED.load(Ordering::Relaxed) && !ptr.is_null() {
            CURRENT_BYTES.fetch_sub(layout.size(), Ordering::Relaxed);
        }
        unsafe { System.dealloc(ptr, layout) };
    }
}

#[derive(Debug)]
struct AllocSnapshot {
    allocs: usize,
    bytes: usize,
    peak_bytes: usize,
}

fn measure<T>(f: impl FnOnce() -> T) -> (T, AllocSnapshot) {
    let _guard = MEASURE_LOCK
        .lock()
        .expect("allocation measure lock poisoned");
    ALLOCS.store(0, Ordering::Relaxed);
    BYTES.store(0, Ordering::Relaxed);
    CURRENT_BYTES.store(0, Ordering::Relaxed);
    PEAK_BYTES.store(0, Ordering::Relaxed);
    ENABLED.store(true, Ordering::Relaxed);
    let result = f();
    ENABLED.store(false, Ordering::Relaxed);
    let snapshot = AllocSnapshot {
        allocs: ALLOCS.load(Ordering::Relaxed),
        bytes: BYTES.load(Ordering::Relaxed),
        peak_bytes: PEAK_BYTES.load(Ordering::Relaxed),
    };
    (result, snapshot)
}

const CSV_RULES: &str = r#"
version: 1
input:
  format: csv
  csv:
    has_header: true
mappings:
  - target: "id"
    source: "input.id"
  - target: "name"
    source: "input.name"
"#;

fn csv_input(record_count: usize) -> String {
    let mut input = String::from("id,name\n");
    for i in 0..record_count {
        input.push_str(&format!("{},item-{}\n", i, i));
    }
    input
}

#[test]
fn stream_peak_memory_grows_slower_than_batch_for_csv() {
    let _test_guard = TEST_LOCK.lock().expect("allocation test lock poisoned");
    let rule = parse_rule_file(CSV_RULES).expect("rule should parse");
    let small = csv_input(1_000);
    let large = csv_input(20_000);

    let (_, small_stream) = measure(|| {
        let stream = transform_stream(&rule, &small, None).expect("stream should start");
        stream
            .map(|item| item.expect("stream item should transform"))
            .filter(|item| item.output.is_some())
            .count()
    });
    let (_, large_stream) = measure(|| {
        let stream = transform_stream(&rule, &large, None).expect("stream should start");
        stream
            .map(|item| item.expect("stream item should transform"))
            .filter(|item| item.output.is_some())
            .count()
    });
    let (_, small_batch) = measure(|| transform(&rule, &small, None).expect("batch should pass"));
    let (_, large_batch) = measure(|| transform(&rule, &large, None).expect("batch should pass"));

    eprintln!(
        "stream allocation baseline: small={:?} large={:?}; batch baseline: small={:?} large={:?}",
        small_stream, large_stream, small_batch, large_batch
    );

    assert!(
        large_stream.peak_bytes < small_stream.peak_bytes * 4,
        "stream peak should stay near constant: small={:?} large={:?}",
        small_stream,
        large_stream
    );
    assert!(
        large_batch.peak_bytes > small_batch.peak_bytes * 8,
        "batch peak should grow with output records: small={:?} large={:?}",
        small_batch,
        large_batch
    );
}

#[test]
fn simple_json_transform_allocation_per_record_stays_bounded() {
    let _test_guard = TEST_LOCK.lock().expect("allocation test lock poisoned");
    let rule = parse_rule_file(
        r#"
version: 1
input:
  format: json
  json: {}
mappings:
  - target: "id"
    source: "input.id"
  - target: "name"
    source: "input.name"
"#,
    )
    .expect("rule should parse");
    let records: Vec<_> = (0..2_000)
        .map(|i| json!({ "id": i, "name": format!("item-{}", i) }))
        .collect();

    let (_, stats) = measure(|| {
        let mut count = 0usize;
        for record in &records {
            if transform_record(&rule, record, None)
                .expect("record transform should pass")
                .is_some()
            {
                count += 1;
            }
        }
        count
    });
    let bytes_per_record = stats.bytes / 2_000;
    let allocs_per_record = stats.allocs / 2_000;

    eprintln!(
        "json transform allocation baseline: stats={:?} bytes_per_record={} allocs_per_record={}",
        stats, bytes_per_record, allocs_per_record
    );

    assert!(
        bytes_per_record < 1_800,
        "bytes per record should stay bounded: stats={:?}",
        stats
    );
    assert!(
        allocs_per_record < 40,
        "allocations per record should stay bounded: stats={:?}",
        stats
    );
}
