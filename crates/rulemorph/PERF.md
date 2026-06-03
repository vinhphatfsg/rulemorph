# Rulemorph Core Performance Snapshot

| benchmark | mean ns/iter | records/sec | MB/sec | baseline delta | status |
| --- | ---: | ---: | ---: | ---: | --- |
| normalize/csv/records_10k | 2538424 | - | 73.93 | - | new |
| normalize/json/records_10k | 3229341 | - | 128.99 | - | new |
| parse/rule_file_cached/extended | 2487 | - | - | - | new |
| parse/rule_file_cached/lookup | 673 | - | - | - | new |
| parse/rule_file_cached/simple | 400 | - | - | - | new |
| parse/rule_file_cold_like/extended | 84784 | - | - | - | new |
| parse/rule_file_cold_like/lookup | 20074 | - | - | - | new |
| parse/rule_file_cold_like/simple | 10414 | - | - | - | new |
| trace/simple/trace_metadata_only | 5947003 | 168151.93 | - | - | new |
| trace/simple/trace_off | 1131252 | 883976.16 | - | - | new |
| trace/simple/trace_raw | 6058296 | 165062.90 | - | - | new |
| transform/batch/extended/cold_parse_records_5k | 99674915 | 50163.07 | - | - | new |
| transform/batch/extended/hot_records_5k | 99128198 | 50439.73 | - | - | new |
| transform/batch/lookup/records_5k_context_100 | 8057701 | 620524.35 | - | - | new |
| transform/batch/lookup_scale/context_size/10 | 389399 | 642014.25 | - | - | new |
| transform/batch/lookup_scale/context_size/100 | 422121 | 592247.63 | - | - | new |
| transform/batch/lookup_scale/context_size/1000 | 639309 | 391047.36 | - | - | new |
| transform/batch/simple/records_5k | 5352826 | 934085.95 | - | - | new |
| transform/end_to_end/json/batch_transform | 10818537 | 924339.40 | - | - | new |
| transform/end_to_end/json/stream_drain | 11267942 | 887473.51 | - | - | new |
| transform/evaluator/json/record_loop | 14585847 | 685596.12 | - | - | new |
| transform/stream/csv/records_10k | 7598426 | 1316061.99 | - | - | new |
| transform/v2/collection/items_per_record/16 | 26267193 | 19035.15 | - | - | new |
| transform/v2/collection/items_per_record/4 | 4947656 | 101057.96 | - | - | new |
| transform/v2/collection/items_per_record/64 | 239235416 | 2089.99 | - | - | new |
