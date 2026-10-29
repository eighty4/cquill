# builds a cquill image from a crates released cquill version
# use `cquill.build.Dockerfile` to build an image from source

ARG RUST_VERSION=1
FROM rust:${RUST_VERSION}-slim-trixie AS install

ARG CQUILL_VERSION=next
ARG RUST_VERSION
LABEL tech.eighty4.cquill.version=${CQUILL_VERSION}
LABEL tech.eighty4.cquill.rust-version=${RUST_VERSION}
RUN test -n "$CQUILL_VERSION"
RUN cargo install --version $CQUILL_VERSION cquill

FROM gcr.io/distroless/cc
COPY --from=install /usr/local/cargo/bin/cquill /usr/bin/cquill
WORKDIR /cquill
VOLUME /cquill/cql
ENTRYPOINT ["cquill"]
