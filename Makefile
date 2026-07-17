GOBIN := $(shell go env GOPATH)/bin
MODULE := github.com/sanskarpan/resilience
VERSION := $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
LDFLAGS := -ldflags="-s -w -X $(MODULE)/server/version.Version=$(VERSION)"

.DEFAULT_GOAL := help

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

build-go: ## Build the demo server binary
	CGO_ENABLED=0 go build $(LDFLAGS) -o bin/demo-server ./server/

test: ## Run all tests with coverage
	go test -coverprofile=coverage.out ./...
	go tool cover -func=coverage.out | tail -1

test-race: ## Run tests with race detector (required before every PR)
	go test -race -count=3 ./...

test-unit: ## Run unit tests only (fast, no integration)
	go test -short ./...

test-integration: ## Run integration tests (requires Docker)
	go test -tags=integration -run Integration ./...

fuzz: ## Run fuzz tests (30s each)
	@for target in FuzzTokenBucket FuzzGCRA FuzzFixedWindow; do \
		echo "Fuzzing $$target..."; \
		go test -fuzz=$$target -fuzztime=30s ./ratelimit/tokenbucket/ 2>/dev/null || \
		go test -fuzz=$$target -fuzztime=30s ./ratelimit/gcra/ 2>/dev/null || \
		go test -fuzz=$$target -fuzztime=30s ./ratelimit/fixedwindow/ 2>/dev/null || true; \
	done

bench: ## Run all benchmarks
	go test -bench=. -benchmem -count=5 ./... | tee bench-$(VERSION).txt

bench-compare: ## Compare benchmarks: make bench-compare OLD=main NEW=HEAD
	@command -v benchstat >/dev/null || go install golang.org/x/perf/cmd/benchstat@latest
	git stash && git checkout $(OLD) && go test -bench=. -benchmem -count=10 ./... > /tmp/old.txt
	git checkout $(NEW) && git stash pop && go test -bench=. -benchmem -count=10 ./... > /tmp/new.txt
	benchstat /tmp/old.txt /tmp/new.txt

lint: ## Run all linters
	golangci-lint run --timeout 5m

lint-fix: ## Auto-fix lint issues
	golangci-lint run --fix

godoc: ## Serve godoc locally
	@echo "Godoc available at http://localhost:6060/pkg/$(MODULE)/"
	godoc -http=:6060

verify-deps: ## Verify core algorithm packages have zero external runtime dependencies
	@# Core packages (algorithms + pure logic) must have zero external deps.
	@# Adapter packages (middleware/, store/) are allowed to import gRPC/Redis.
	@CORE_PKGS="./ratelimit ./ratelimit/tokenbucket ./ratelimit/gcra ./ratelimit/fixedwindow ./ratelimit/slidingwindow ./ratelimit/leakybucket ./ratelimit/adaptive ./ratelimit/composite ./circuitbreaker ./bulkhead ./retry ./retry/backoff ./timeout ./fallback ./pipeline ./internal/clock ./internal/atomicx"; \
	FAILED=0; \
	for pkg in $$CORE_PKGS; do \
		if [ -d "$$pkg" ]; then \
			DEPS=$$(go list -f '{{join .Imports "\n"}}' $$pkg 2>/dev/null | \
				grep -v "^$(MODULE)" | grep "\." | grep -v "^testing\b" | grep -v "^internal/" || true); \
			if [ -n "$$DEPS" ]; then echo "FAIL $$pkg has external deps: $$DEPS"; FAILED=1; \
			else echo "OK $$pkg"; fi; \
		fi; \
	done; \
	exit $$FAILED

docker: ## Build Docker image
	docker build -t resilience-demo:$(VERSION) .

docker-run: ## Start full stack with Docker Compose
	docker-compose up --build

clean: ## Remove build artifacts
	rm -rf bin/ coverage.out bench-*.txt
