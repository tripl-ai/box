cargo +nightly run \
--release \
--features "simd snmalloc" \
--bin box \
-- \
execute --job-path "./job.json" INPUT=./tpch/parquet
