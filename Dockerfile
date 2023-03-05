FROM rust:1.67 as builder
WORKDIR /cquill
COPY . .
RUN cargo build --profile release

FROM gcr.io/distroless/cc
COPY --from=builder /cquill/target/release/cquill /cquill
CMD ["/cquill"]
