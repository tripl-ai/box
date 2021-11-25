FROM rust:1.56.0 as build
COPY . /src
RUN cd /src && \
    apt-get update && \
    apt-get install -y cmake && \
    rustup toolchain install nightly && \
    rustup component add rustfmt && \
    cargo +nightly build --release --features "simd snmalloc vendored-zmq"

FROM jupyter/base-notebook:latest

COPY --from=build /src/target/release/box /tmp
RUN /tmp/box install

COPY box.ipynb .