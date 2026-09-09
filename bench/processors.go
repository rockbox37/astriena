package bench

import (
	"context"
	"fmt"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/processor/tailsamplingprocessor"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/processor/processortest"

	"github.com/rockbox37/astriena/components/astrienasampler"
)

// procCfg is the shared keep policy and buffer bound both sides must honor.
// DecisionWait is interpreted by each processor's own clock: Astriena expires
// inline + on a ticker; stock tailsamplingprocessor decides on its event-loop
// tick (default 1s) once DecisionWait has elapsed.
type procCfg struct {
	decisionWait time.Duration
	maxTraces    int
}

func keepPolicyAstriena() []astrienasampler.PolicyCfg {
	return []astrienasampler.PolicyCfg{
		{Type: "status_code", Keep: "ERROR"},
		{Type: "latency", ThresholdMS: 200},
	}
}

func keepPolicyStockYAML() []any {
	return []any{
		map[string]any{
			"name": "status_code",
			"type": "status_code",
			"status_code": map[string]any{
				"status_codes": []any{"ERROR"},
			},
		},
		map[string]any{
			"name": "latency",
			"type": "latency",
			"latency": map[string]any{
				"threshold_ms": 200,
			},
		},
	}
}

func newAstriena(next consumer.Traces, cfg procCfg) (processor.Traces, error) {
	factory := astrienasampler.NewFactory()
	set := processortest.NewNopSettings(factory.Type())
	acfg := &astrienasampler.Config{
		DecisionWait: cfg.decisionWait,
		MaxTraces:    cfg.maxTraces,
		Policies:     keepPolicyAstriena(),
	}
	p, err := factory.CreateTraces(context.Background(), set, acfg, next)
	if err != nil {
		return nil, fmt.Errorf("astriena CreateTraces: %w", err)
	}
	if err := p.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		return nil, fmt.Errorf("astriena Start: %w", err)
	}
	return p, nil
}

func newStock(next consumer.Traces, cfg procCfg) (processor.Traces, error) {
	factory := tailsamplingprocessor.NewFactory()
	numTraces := cfg.maxTraces
	if numTraces < 1 {
		numTraces = 1
	}
	raw := map[string]any{
		"decision_wait":               cfg.decisionWait.String(),
		"num_traces":                  numTraces,
		"expected_new_traces_per_sec": uint64(10_000),
		"sample_on_first_match":       true,
		// span-ingest evaluates on the event loop as batches arrive, matching
		// Astriena's ingest-time policy pass. The default trace-complete
		// strategy only decides on a 1s ticker, which would make a keep-parity
		// check wait seconds and would leave the memory fill racing the clock.
		"sampling_strategy": "span-ingest",
		"policies":          keepPolicyStockYAML(),
	}
	def := factory.CreateDefaultConfig()
	if err := confmap.NewFromStringMap(raw).Unmarshal(def); err != nil {
		return nil, fmt.Errorf("stock config: %w", err)
	}
	set := processortest.NewNopSettings(factory.Type())
	p, err := factory.CreateTraces(context.Background(), set, def, next)
	if err != nil {
		return nil, fmt.Errorf("stock CreateTraces: %w", err)
	}
	if err := p.Start(context.Background(), componenttest.NewNopHost()); err != nil {
		return nil, fmt.Errorf("stock Start: %w", err)
	}
	return p, nil
}

func shutdown(p processor.Traces) {
	if err := p.Shutdown(context.Background()); err != nil {
		panic(fmt.Errorf("processor Shutdown: %w", err))
	}
}

// drainStock waits for the stock processor's workChan event loop to absorb
// the last enqueued batches. ConsumeTraces copies and sends; the buffer insert
// is queued onto workChan (buffered to GOMAXPROCS). After the last send at
// most a small number of batches remain queued, so a short pause is enough.
func drainStock() {
	time.Sleep(100 * time.Millisecond)
}
