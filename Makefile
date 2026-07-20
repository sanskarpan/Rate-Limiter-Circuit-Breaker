GOBIN := $(shell go env GOPATH)/bin
MODULE := github.com/sanskarpan/Rate-Limiter-Circuit-Breaker
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

fuzz-server: ## Fuzz the demo server's JSON decoders & simulator (§7.5, 15s each)
	@for target in FuzzHandleAllow FuzzHandleCBExecute FuzzHandleSimulate FuzzClampSimulateRequest; do \
		echo "Fuzzing $$target..."; \
		go test -run=xxx -fuzz=$$target$$ -fuzztime=15s ./server/api/ || exit 1; \
	done

bench: ## Run all benchmarks
	go test -bench=. -benchmem -count=5 ./... | tee bench-$(VERSION).txt

bench-compare: ## Compare benchmarks: make bench-compare OLD=main NEW=HEAD
	@command -v benchstat >/dev/null || go install golang.org/x/perf/cmd/benchstat@latest
	git stash && git checkout $(OLD) && go test -bench=. -benchmem -count=10 ./... > /tmp/old.txt
	git checkout $(NEW) && git stash pop && go test -bench=. -benchmem -count=10 ./... > /tmp/new.txt
	benchstat /tmp/old.txt /tmp/new.txt

# ─── Benchmark regression gate (mirrors .github/workflows/bench.yml) ──────────
# Bounded set of hot-path benchmarks that the CI regression gate runs. Kept in
# one variable so the Makefile and CI stay in sync.
BENCH_PKGS := ./ratelimit/tokenbucket/ ./ratelimit/gcra/ ./circuitbreaker/
BENCH_COUNT ?= 8
BENCH_TIME ?= 200ms
BENCH_OUT ?= /tmp/bench-ci.txt

bench-ci: ## Run the bounded CI benchmark set once (hot rate limiters + circuit breaker) → $(BENCH_OUT)
	go test -bench=. -benchmem -run='^$$' -count=$(BENCH_COUNT) -benchtime=$(BENCH_TIME) $(BENCH_PKGS) | tee $(BENCH_OUT)

bench-stat: ## Compare two benchstat inputs: make bench-stat BASE=base.txt HEAD=head.txt
	@command -v benchstat >/dev/null || go install golang.org/x/perf/cmd/benchstat@latest
	benchstat $(BASE) $(HEAD)

vuln: ## Scan for known vulnerabilities (govulncheck)
	go run golang.org/x/vuln/cmd/govulncheck@latest ./...

# Mutation testing (ENHANCEMENTS §6.6). Uses go-gremlins via `go run` so NO
# dependency is added to the core go.mod/go.sum. Slow — run locally or nightly,
# NOT in required PR checks. Config lives in .gremlins.yaml. Targets only the
# core algorithm packages by default; override with PKG=./retry (etc.).
#   make mutation              # run on the default algorithm package set
#   make mutation PKG=./ratelimit/tokenbucket
# See docs/mutation-testing.md for how to interpret survivors.
MUTATION_TOOL := github.com/go-gremlins/gremlins/cmd/gremlins@v0.5.0
PKG ?= ./circuitbreaker/... ./retry/... ./ratelimit/tokenbucket/... ./ratelimit/gcra/... ./ratelimit/fixedwindow/... ./ratelimit/slidingwindow/... ./ratelimit/leakybucket/... ./bulkhead/...

mutation: ## Run mutation testing on core algorithm packages (§6.6, slow; go run, no core dep)
	@echo "Running mutation testing (go-gremlins) on: $(PKG)"
	@echo "This is slow. See docs/mutation-testing.md for interpretation."
	go run $(MUTATION_TOOL) unleash --config .gremlins.yaml $(PKG)

mutation-dry: ## List mutants without running tests (fast sanity check of coverage)
	go run $(MUTATION_TOOL) unleash --dry-run --config .gremlins.yaml $(PKG)

test-e2e: ## Run Playwright E2E: builds+starts server (:8080) & frontend (:3000), then runs tests
	@echo "Building demo server..."
	@CGO_ENABLED=0 go build -o bin/demo-server ./server/
	@echo "Starting demo server on :8080..."
	@./bin/demo-server > /tmp/demo-server.log 2>&1 & echo $$! > /tmp/demo-server.pid
	@for i in $$(seq 1 60); do curl -fsS http://localhost:8080/health/live >/dev/null 2>&1 && break; sleep 1; done
	@echo "Building & starting frontend on :3000..."
	@cd frontend && npm ci && npx playwright install --with-deps chromium && npm run build
	@cd frontend && NEXT_PUBLIC_API_URL=http://localhost:8080 npm run start > /tmp/frontend.log 2>&1 & echo $$! > /tmp/frontend.pid
	@for i in $$(seq 1 60); do curl -fsS http://localhost:3000 >/dev/null 2>&1 && break; sleep 1; done
	@cd frontend && npm run test:e2e; status=$$?; \
		kill `cat /tmp/demo-server.pid` `cat /tmp/frontend.pid` 2>/dev/null || true; \
		exit $$status

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
	@CORE_PKGS="./ratelimit ./ratelimit/tokenbucket ./ratelimit/gcra ./ratelimit/fixedwindow ./ratelimit/slidingwindow ./ratelimit/leakybucket ./ratelimit/adaptive ./ratelimit/composite ./circuitbreaker ./bulkhead ./retry ./retry/backoff ./timeout ./fallback ./pipeline ./internal/clock ./internal/atomicx ./loadshed ./concurrency ./metric ./debounce ./ratelimit/tiered ./resiliencex ./logging ./eventstream ./resilience"; \
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

test-contrib: ## Build+test the contrib framework-middleware module
	cd contrib && go build ./... && go vet ./... && go test -race ./...

test-stores: ## Build+test the stores backend module (memcached, dynamodb)
	cd stores && go build ./... && go vet ./... && go test -race ./...
