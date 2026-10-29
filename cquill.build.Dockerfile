# builds a cquill image from source
# use `cquill.install.Dockerfile` to build an image from a crates released cquill version

ARG RUST_VERSION=1
FROM rust:${RUST_VERSION}-slim-trixie AS build

ARG RUST_VERSION
LABEL tech.eighty4.cquill.version=build
LABEL tech.eighty4.cquill.rust-version=${RUST_VERSION}
WORKDIR /cquill
COPY . .
RUN cargo build --profile release

FROM gcr.io/distroless/cc
COPY --from=build /cquill/target/release/cquill /usr/bin/cquill
WORKDIR /cquill
VOLUME /cquill/cql
ENTRYPOINT ["cquill"]
