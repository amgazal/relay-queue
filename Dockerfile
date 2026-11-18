FROM --platform=$BUILDPLATFORM golang:1.27 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download && go mod verify
COPY cmd ./cmd
COPY internal ./internal
ARG TARGETARCH
RUN --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=linux GOARCH=$TARGETARCH go build -mod=readonly -trimpath -buildvcs=false -ldflags="-s -w" -o /out/ ./cmd/...

FROM scratch AS runtime
LABEL org.opencontainers.image.source="https://github.com/amgazal/relay-queue"
COPY --from=build /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
USER 65532:65532

FROM runtime AS api
COPY --from=build /out/relay-api /relay-api
ENTRYPOINT ["/relay-api"]

FROM runtime AS worker
COPY --from=build /out/relay-worker /relay-worker
ENTRYPOINT ["/relay-worker"]

FROM runtime AS receiver
COPY --from=build /out/relay-demo-receiver /relay-demo-receiver
ENTRYPOINT ["/relay-demo-receiver"]
