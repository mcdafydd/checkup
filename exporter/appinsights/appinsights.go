package appinsights

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/microsoft/ApplicationInsights-Go/appinsights"
	"github.com/sourcegraph/checkup/types"
)

// Type should match the package name
const Type = "appinsights"

// Exporter implements a Exporter by sending Checker output to an external telemetry tool
type Exporter struct {
	// InstrumentationKey is a GUID used to send trackAvailability()
	// telemetry to Application Insights
	InstrumentationKey string `json:"instrumentation_key"`

	// TestName identifies the test name sent
	// in Application Insights trackAvailability() events
	TestName string `json:"test_name,omitempty"`

	// TestLocation identifies the test location sent
	// in Application Insights trackAvailability() events
	TestLocation string `json:"test_location,omitempty"`

	// TelemetryClient is the appinsights.Client with which to
	// send Application Insights trackAvailability() events
	// Automatically created if InstrumentationKey is set.
	TelemetryClient appinsights.TelemetryClient `json:"-"`
}

// New creates a new Exporter instance based on json config
func New(config json.RawMessage) (Exporter, error) {
	var exporter Exporter
	err := json.Unmarshal(config, &exporter)
	if exporter.TestLocation == "" {
		exporter.TestLocation = "Checkup Exporter"
	}
	exporter.TelemetryClient = appinsights.NewTelemetryClient(exporter.InstrumentationKey)
	return exporter, err
}

// Type returns the logger package name
func (Exporter) Type() string {
	return Type
}

// Export takes a list of Checker results and sends them to the configured
// Application Insights instance.
func (c Exporter) Export(results []types.Result) error {
	errs := make(types.Errors, 0)
	for _, result := range results {
		if err := c.Send(result); err != nil {
			errs = append(errs, err)
		}
	}
	return errs
}

// Send sends a result to the exporter
func (c Exporter) Send(conclude types.Result) error {
	attempts := len(conclude.Times)
	rtts := make([]string, attempts)
	message := conclude.Notice
	if conclude.Degraded || conclude.Down {
		for i := 0; i < attempts; i++ {
			rtts[i] = conclude.Times[i].RTT.String()
		}
		message = fmt.Sprintf("%s - Number of attempts = %d (%s)", message, len(conclude.Times), strings.Join(rtts, " "))
	}

	availability := appinsights.NewAvailabilityTelemetry(conclude.Title, conclude.Stats.Mean, conclude.Healthy)
	availability.RunLocation = c.TestLocation
	availability.Message = message

	// Submit the telemetry
	c.TelemetryClient.Track(availability)
	return nil
}
