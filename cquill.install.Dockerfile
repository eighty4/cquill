# builds a cquill image from a crates released cquill version
# use `cquill.build.Dockerfile` to build an image from source

FROM rust:1-slim-bullseye AS install
ARG CQUILL_VERSION=next
RUN test -n "$CQUILL_VERSION"
RUN cargo install --version $CQUILL_VERSION cquill

FROM gcr.io/distroless/cc
COPY --from=install /usr/local/cargo/bin/cquill /usr/bin/cquill
WORKDIR /cquill
VOLUME /cquill/cql
ENTRYPOINT ["cquill"]
