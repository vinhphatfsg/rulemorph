# Rulemorph Core Performance Snapshot

| benchmark | mean ns/iter | records/sec | MB/sec | baseline delta | status |
| --- | ---: | ---: | ---: | ---: | --- |
| normalize/csv/records_10k | 2625253 | - | 71.49 | +0.00% | ok |
| normalize/json/records_10k | 4792919 | - | 86.91 | +0.00% | ok |
| parse/rule_file_cached/extended | 2445 | - | - | +0.00% | ok |
| parse/rule_file_cached/lookup | 678 | - | - | +0.00% | ok |
| parse/rule_file_cached/simple | 386 | - | - | +0.00% | ok |
| parse/rule_file_cold_like/extended | 83194 | - | - | +0.00% | ok |
| parse/rule_file_cold_like/lookup | 19831 | - | - | +0.00% | ok |
| parse/rule_file_cold_like/simple | 10143 | - | - | +0.00% | ok |
| trace/simple/trace_metadata_only | 20618281 | 48500.65 | - | +0.00% | ok |
| trace/simple/trace_off | 1819785 | 549515.55 | - | +0.00% | ok |
| trace/simple/trace_raw | 21506644 | 46497.26 | - | +0.00% | ok |
| transform/batch/extended/cold_parse_records_5k | 126790549 | 39435.12 | - | +0.00% | ok |
| transform/batch/extended/hot_records_5k | 129421364 | 38633.50 | - | +0.00% | ok |
| transform/batch/lookup/records_5k_context_100 | 145282093 | 34415.80 | - | +0.00% | ok |
| transform/batch/lookup_scale/context_size/10 | 1437107 | 173960.63 | - | +0.00% | ok |
| transform/batch/lookup_scale/context_size/100 | 7041687 | 35502.86 | - | +0.00% | ok |
| transform/batch/lookup_scale/context_size/1000 | 62139345 | 4023.22 | - | +0.00% | ok |
| transform/batch/simple/records_5k | 10072816 | 496385.52 | - | +0.00% | ok |
| transform/end_to_end/json/batch_transform | 19096434 | 523657.97 | - | +0.00% | ok |
| transform/end_to_end/json/stream_drain | 18634890 | 536627.81 | - | +0.00% | ok |
| transform/evaluator/json/record_loop | 13709661 | 729412.65 | - | +0.00% | ok |
| transform/stream/csv/records_10k | 10914281 | 916230.77 | - | +0.00% | ok |
| transform/v2/collection/items_per_record/16 | 25542453 | 19575.25 | - | +0.00% | ok |
| transform/v2/collection/items_per_record/4 | 5306637 | 94221.64 | - | +0.00% | ok |
| transform/v2/collection/items_per_record/64 | 236207631 | 2116.78 | - | +0.00% | ok |
