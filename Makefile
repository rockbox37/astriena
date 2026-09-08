# Astriena — build targets.
#
# The pure core (internal/...) builds and tests offline with plain `go`.
# The full distribution is assembled by the OpenTelemetry Collector Builder,
# which fetches and pins the Collector modules named in builder-config.yaml.

OCB_VERSION ?= 0.116.0
GOBIN       := $(shell go env GOPATH)/bin
BUILDER     := $(GOBIN)/builder
BUILDER_CFG := builder-config.yaml

.PHONY: help
help:
	@echo "astriena make targets:"
	@echo "  make test     - unit-test the pure core (offline)"
	@echo "  make lint     - go vet the pure core (offline)"
	@echo "  make tidy     - go mod tidy"
	@echo "  make builder  - install the OpenTelemetry Collector Builder"
	@echo "  make build    - build the astriena distribution into ./_build"
	@echo "  make run      - run the built binary with config.yaml"

.PHONY: test
test:
	go test ./internal/...

.PHONY: lint
lint:
	go vet ./internal/...

.PHONY: tidy
tidy:
	go mod tidy

.PHONY: builder
builder:
	go install go.opentelemetry.io/collector/cmd/builder@v$(OCB_VERSION)

.PHONY: build
build: builder
	$(BUILDER) --config $(BUILDER_CFG)

.PHONY: run
run:
	./_build/astriena --config config.yaml
