BINARY := rollouts-plugin-metric-datadog
PLUGIN_PATH := mubarak-j/rollouts-plugin-metric-datadog
DIST := dist

.PHONY: build release test vet tidy
# WARNING: go mod tidy strips the cgo-only github.com/DataDog/zstd dep and
# breaks the -race build. After running tidy, restore it via:
#   go get github.com/DataDog/zstd@v1.5.2
tidy:
	go mod tidy

build:
	CGO_ENABLED=0 go build -ldflags="-s -w" -trimpath -o $(DIST)/$(BINARY) .

release:
	CGO_ENABLED=0 GOOS=linux  GOARCH=amd64 go build -ldflags="-s -w" -trimpath -o $(DIST)/$(BINARY)-linux-amd64  .
	CGO_ENABLED=0 GOOS=linux  GOARCH=arm64 go build -ldflags="-s -w" -trimpath -o $(DIST)/$(BINARY)-linux-arm64  .
	CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build -ldflags="-s -w" -trimpath -o $(DIST)/$(BINARY)-darwin-amd64 .
	CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -ldflags="-s -w" -trimpath -o $(DIST)/$(BINARY)-darwin-arm64 .

test:
	go test ./... -race -count=1

vet:
	go vet ./...
