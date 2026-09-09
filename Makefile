# Astriena — build targets.
#
# The pure core (internal/...) builds and tests offline with plain `go`.
# The full distribution is assembled by the OpenTelemetry Collector Builder,
# which fetches and pins the Collector modules named in builder-config.yaml.

OCB_VERSION ?= 0.160.0
GOBIN       := $(shell go env GOPATH)/bin
BUILDER     := $(GOBIN)/builder
BUILDER_CFG := builder-config.yaml

.PHONY: help
help:
	@echo "astriena make targets:"
	@echo "  make test       - unit-test the pure core (offline)"
	@echo "  make test-bench - cheap head-to-head verification (Collector/contrib; see docs/benchmarks.md)"
	@echo "  make bench      - benchmark the sampling engine (offline; see docs/benchmarks.md)"
	@echo "  make bench-h2h  - Astriena vs stock tail_sampling (Collector/contrib; 20k traces)"
	@echo "  make lint       - go vet the pure core (offline)"
	@echo "  make tidy       - go mod tidy (root + bench)"
	@echo "  make builder  - install the OpenTelemetry Collector Builder"
	@echo "  make build    - build the astriena distribution into ./_build"
	@echo "  make run      - run the built binary with config.yaml"

.PHONY: test
test:
	go test ./internal/...

.PHONY: test-bench
test-bench:
	go test -C bench -short ./...

.PHONY: bench
bench:
	go test ./internal/sampling/ -run '^$$' -bench . -benchmem -benchtime=200000x

.PHONY: bench-h2h
bench-h2h:
	go test -C bench -run '^$$' -bench . -benchmem -benchtime=20000x

.PHONY: lint
lint:
	go vet ./internal/...

.PHONY: tidy
tidy:
	go mod tidy
	go -C bench mod tidy

.PHONY: builder
builder:
	go install go.opentelemetry.io/collector/cmd/builder@v$(OCB_VERSION)

.PHONY: build
build: builder
	$(BUILDER) --config $(BUILDER_CFG)

.PHONY: run
run:
	./_build/astriena --config config.yaml
