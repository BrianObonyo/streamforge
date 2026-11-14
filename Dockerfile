# syntax=docker/dockerfile:1
# One image, four binaries (producer, processor, api, demo); pick with the command.
FROM golang:1.22-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
# -tags redpanda links the Kafka-protocol adapter; CGO off gives a static binary.
RUN CGO_ENABLED=0 go build -tags redpanda -trimpath -ldflags="-s -w" -o /out/ ./cmd/...

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
USER nonroot:nonroot
ENTRYPOINT ["/usr/local/bin/processor"]
