FROM golang:1.24-alpine AS builder

WORKDIR /app

RUN apk add --no-cache git ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN go build -o main ./cmd/

FROM alpine:3.18
WORKDIR /app
COPY --from=builder /app/main .
CMD ["./main"]

