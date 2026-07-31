# syntax=docker/dockerfile:1

FROM golang:1.26.5-alpine AS builder
WORKDIR /src

ARG GOPROXY=https://proxy.golang.org,direct
COPY go.mod go.sum ./
RUN GOPROXY=${GOPROXY} go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -trimpath -ldflags="-s -w" -o /out/goai .

FROM alpine:3.21
RUN apk add --no-cache ca-certificates tzdata \
    && addgroup -S goai \
    && adduser -S -G goai goai

COPY --from=builder /out/goai /usr/local/bin/goai

USER goai
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/goai"]
