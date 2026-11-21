# KeyShift developer commands. Run `make help` for a summary.

GO        ?= go
NPM       ?= npm
BIN       := bin
PKGS      := ./...
GOLANGCI  ?= $(shell command -v golangci-lint 2>/dev/null || echo $(HOME)/go/bin/golangci-lint)
FUZZTIME  ?= 15s
COMPOSE   ?= docker compose
TEST_COMPOSE := $(COMPOSE) -f deploy/docker/docker-compose.test.yml
LAB_COMPOSE  := $(COMPOSE) -f deploy/docker/docker-compose.yml
VERSION   ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo dev)
LDFLAGS   := -s -w

# Integration test endpoints (started by test-infra-up).
export KEYSHIFT_TEST_REDIS_ADDR         ?= localhost:16379
export KEYSHIFT_TEST_RESTART_ADDR       ?= localhost:16380
export KEYSHIFT_TEST_RESTART_CONTAINER  ?= keyshift-test-redis-restart
export KEYSHIFT_TEST_REDIS_OLD_ADDR     ?= localhost:16381
export KEYSHIFT_TEST_CLUSTER_ADDRS      ?= 127.0.0.1:17001,127.0.0.1:17002,127.0.0.1:17003

.PHONY: help fmt fmt-check vet lint test test-race test-integration test-fuzz bench bench-lab simulate \
        build web docker-build docker-up docker-down test-infra-up test-infra-down vuln clean ci

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-18s\033[0m %s\n", $$1, $$2}'

fmt: ## Format Go and TypeScript sources
	gofmt -w $$(git ls-files '*.go' 2>/dev/null || find . -name '*.go' -not -path './web/node_modules/*')

fmt-check: ## Fail if Go sources are not gofmt-formatted
	@out=$$(gofmt -l $$(find . -name '*.go' -not -path './web/node_modules/*')); \
	if [ -n "$$out" ]; then echo "gofmt needed:"; echo "$$out"; exit 1; fi

vet: ## Run go vet (including integration-tagged files)
	$(GO) vet $(PKGS)
	$(GO) vet -tags integration $(PKGS)

lint: fmt-check vet ## Run gofmt check, go vet, golangci-lint, and the dashboard type check
	$(GOLANGCI) run $(PKGS)
	cd web && $(NPM) run typecheck

test: ## Run unit tests
	$(GO) test -count=1 $(PKGS)

test-race: ## Run unit tests with the race detector
	$(GO) test -race -count=1 $(PKGS)

test-infra-up: ## Start Redis containers used by integration tests
	$(TEST_COMPOSE) up -d --wait

test-infra-down: ## Stop integration test containers
	$(TEST_COMPOSE) down -v

test-integration: test-infra-up ## Run integration and fault tests against real Redis (Docker)
	# -p 1: packages share Redis servers and some tests FLUSHALL or count
	# server-wide tracking state, so integration packages run one at a time.
	$(GO) test -tags integration -race -count=1 -p 1 -timeout 20m $(PKGS)

test-fuzz: ## Run every fuzz target for FUZZTIME each
	@set -e; for target in \
	  "./internal/detector FuzzDetector" \
	  "./internal/fingerprint FuzzFingerprint" \
	  "./internal/policy FuzzParse" \
	  "./internal/policy FuzzGlob" \
	  "./internal/telemetry FuzzDecode"; do \
	  set -- $$target; echo "== $$2 ($$1)"; \
	  $(GO) test -run '^$$' -fuzz "^$$2$$" -fuzztime $(FUZZTIME) $$1; \
	done

bench: ## Run Go micro-benchmarks (detector, admission, fingerprint, counters)
	$(GO) test -run '^$$' -bench . -benchmem -benchtime 1s ./internal/...

bench-lab: test-infra-up build ## Run the scenario benchmark suite against the test Redis (FLUSHALL!)
	$(BIN)/keyshift bench -redis $(KEYSHIFT_TEST_REDIS_ADDR) -cluster $(KEYSHIFT_TEST_CLUSTER_ADDRS) -yes \
	  -restart-container $(KEYSHIFT_TEST_RESTART_CONTAINER) -json benchmarks/results/latest.json \
	  -markdown benchmarks/results/latest.md

simulate: build ## Run the 5,000-instance logical controller simulation
	$(BIN)/keyshift simulate

web: ## Build the dashboard into web/dist (embedded by the controller)
	cd web && $(NPM) ci --no-audit --no-fund && $(NPM) run build

build: ## Build all binaries into bin/ (builds the dashboard first if npm is available)
	@if command -v $(NPM) >/dev/null 2>&1; then $(MAKE) web; else echo "npm not found: controller built without dashboard"; fi
	$(GO) build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN)/keyshift ./cmd/keyshift
	$(GO) build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN)/keyshift-controller ./cmd/keyshift-controller
	$(GO) build -trimpath -ldflags '$(LDFLAGS)' -o $(BIN)/keyshift-bench ./cmd/keyshift-bench
	$(GO) build ./examples/...

docker-build: ## Build container images
	docker build -f deploy/docker/Dockerfile -t keyshift/controller:$(VERSION) --target controller .
	docker build -f deploy/docker/Dockerfile -t keyshift/lab:$(VERSION) --target lab .

docker-up: ## Start the local lab: Redis, controller with dashboard, demo workload
	$(LAB_COMPOSE) up -d --build --wait
	@echo "Dashboard: http://localhost:8080  (viewer token: see deploy/docker/lab.env)"

docker-down: ## Stop the local lab
	$(LAB_COMPOSE) down -v

vuln: ## Scan dependencies for known vulnerabilities
	$(GO) run golang.org/x/vuln/cmd/govulncheck@latest $(PKGS)

ci: lint test-race build ## What CI runs without Docker

clean: ## Remove build outputs
	rm -rf $(BIN) web/dist/assets web/dist/index.html
