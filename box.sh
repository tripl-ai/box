cargo +nightly-2021-10-23 run \
--release \
--features "simd snmalloc" \
--bin box \
-- \
execute --job-path "./job.json" INPUT=./tpch/parquet
