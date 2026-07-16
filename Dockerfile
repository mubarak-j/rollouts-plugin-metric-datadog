FROM golang:1.26 AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -o /out/rollouts-plugin-metric-datadog .

# Minimal image whose only job is to expose the binary for an initContainer copy.
FROM alpine:3.24
COPY --from=build /out/rollouts-plugin-metric-datadog /plugin/rollouts-plugin-metric-datadog
