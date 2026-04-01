BIN     := ./tmp/worker
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS := -s -w -X main.version=$(VERSION)

.DEFAULT_GOAL := help

.PHONY: build
build: ## Build the binary locally
	go build -ldflags="$(LDFLAGS)" -trimpath -o $(BIN) ./cmd/worker

.PHONY: run
run: build ## Build and run the worker locally
	$(BIN)

.PHONY: test
test: ## Run tests with race detector
	go test -race ./...

.PHONY: cover
cover: ## Run tests and open HTML coverage report
	go test -race -coverprofile=coverage.out ./...
	go tool cover -html=coverage.out

.PHONY: lint
lint: ## Run golangci-lint (requires golangci-lint installed)
	golangci-lint run --timeout=5m

.PHONY: vet
vet: ## Run go vet
	go vet ./...

.PHONY: tidy
tidy: ## Tidy and verify go modules
	go mod tidy
	go mod verify

.PHONY: image
image: ## Build the production Docker image locally
	docker build \
	  --target prod \
	  --build-arg VERSION=$(VERSION) \
	  --build-arg BUILD_DATE=$(shell date -u +%Y-%m-%dT%H:%M:%SZ) \
	  --build-arg VCS_REF=$(shell git rev-parse HEAD 2>/dev/null || echo unknown) \
	  -f docker/Dockerfile \
	  -t worker-service:$(VERSION) \
	  .

.PHONY: clean
clean: ## Remove build artefacts
	rm -rf tmp/ coverage.out

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-14s\033[0m %s\n", $$1, $$2}'
