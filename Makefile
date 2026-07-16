BINARY := rollouts-plugin-metric-datadog
PLUGIN_PATH := mubarak-j/rollouts-plugin-metric-datadog

.PHONY: build test vet tidy
# WARNING: go mod tidy strips the cgo-only github.com/DataDog/zstd dep and
# breaks the -race build. After running tidy, restore it via:
#   go get github.com/DataDog/zstd@v1.5.2
tidy:
	go mod tidy

build:
	CGO_ENABLED=0 go build -o dist/$(BINARY) .

test:
	go test ./... -race -count=1

vet:
	go vet ./...
