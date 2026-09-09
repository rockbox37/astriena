// Package bench is the head-to-head of Astriena's astriena_sampler adapter
// against contrib tailsamplingprocessor.
//
// It is a separate Go module so the root module stays free of Collector /
// pdata / contrib dependencies (that is what keeps the isolation benches in
// internal/sampling offline and fast). Methodology and numbers live in
// docs/benchmarks.md; run with `make bench-h2h` or `make test-bench`.
package bench
