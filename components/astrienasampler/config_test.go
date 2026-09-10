package astrienasampler

import (
	"strings"
	"testing"
)

func TestConfigValidate(t *testing.T) {
	tests := []struct {
		name    string
		cfg     Config
		wantErr string
	}{
		{
			name: "empty policies is default-drop only",
			cfg:  Config{},
		},
		{
			name: "status_code ok",
			cfg:  Config{Policies: []PolicyCfg{{Type: "status_code", Keep: "ERROR"}}},
		},
		{
			name: "latency ok",
			cfg:  Config{Policies: []PolicyCfg{{Type: "latency", ThresholdMS: 200}}},
		},
		{
			name: "both implemented types ok",
			cfg: Config{Policies: []PolicyCfg{
				{Type: "status_code", Keep: "ERROR"},
				{Type: "latency", ThresholdMS: 200},
			}},
		},
		{
			name:    "unknown type",
			cfg:     Config{Policies: []PolicyCfg{{Type: "probabilistic"}}},
			wantErr: `unknown type "probabilistic"`,
		},
		{
			name:    "empty type",
			cfg:     Config{Policies: []PolicyCfg{{Type: ""}}},
			wantErr: "type is required",
		},
		{
			name:    "status_code missing keep",
			cfg:     Config{Policies: []PolicyCfg{{Type: "status_code"}}},
			wantErr: "status_code requires keep",
		},
		{
			name:    "latency zero threshold",
			cfg:     Config{Policies: []PolicyCfg{{Type: "latency", ThresholdMS: 0}}},
			wantErr: "latency requires threshold_ms > 0",
		},
		{
			name:    "latency negative threshold",
			cfg:     Config{Policies: []PolicyCfg{{Type: "latency", ThresholdMS: -1}}},
			wantErr: "latency requires threshold_ms > 0",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.cfg.Validate()
			if tt.wantErr == "" {
				if err != nil {
					t.Fatalf("Validate() = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("Validate() = nil, want error containing %q", tt.wantErr)
			}
			if !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Validate() = %q, want substring %q", err.Error(), tt.wantErr)
			}
		})
	}
}

func TestDefaultConfigValidates(t *testing.T) {
	cfg := createDefaultConfig().(*Config)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("default config: %v", err)
	}
}
