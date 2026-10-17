# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /src

# Cache dependencies
COPY go.mod ./
# (go.sum if present)
RUN go mod download || true

COPY . .

RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/aegis ./cmd/aegis
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/mockupstream ./cmd/mockupstream
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /bin/benchmark ./cmd/benchmark

# Production runtime stage
FROM alpine:3.20

RUN apk add --no-cache ca-certificates

WORKDIR /app

COPY --from=builder /bin/aegis /app/aegis
COPY --from=builder /bin/mockupstream /app/mockupstream
COPY --from=builder /bin/benchmark /app/benchmark

EXPOSE 8080 8081

ENTRYPOINT ["/app/aegis"]
