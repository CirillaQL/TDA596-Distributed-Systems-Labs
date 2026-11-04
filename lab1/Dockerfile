FROM --platform=$BUILDPLATFORM golang:1.25.1-alpine AS build

ARG TARGETOS
ARG TARGETARCH

WORKDIR /src
COPY go.mod ./
COPY lab1/ ./lab1/
RUN go test ./lab1/... && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/http_server ./lab1 && \
    mkdir /out/data

FROM alpine:3.22

COPY --from=build /out/http_server /http_server
COPY --from=build --chown=65532:65532 /out/data /data
WORKDIR /data
USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/http_server"]
CMD ["8080"]
