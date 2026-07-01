BINARY := rollouts-plugin-metric-datadog
PLUGIN_PATH := mubarak-j/rollouts-plugin-metric-datadog

.PHONY: build test vet tidy
tidy:
	go mod tidy

build:
	CGO_ENABLED=0 go build -o dist/$(BINARY) .

test:
	go test ./... -race -count=1

vet:
	go vet ./...
