# builds a cquill image from source
# use `cquill.install.Dockerfile` to build an image from a crates released cquill version

FROM rust:1-slim-bullseye as build
WORKDIR /cquill
COPY . .
RUN cargo build --profile release

FROM gcr.io/distroless/cc
COPY --from=build /cquill/target/release/cquill /usr/bin/cquill
WORKDIR /cquill
VOLUME /cquill/cql
ENTRYPOINT ["cquill"]
