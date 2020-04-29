package exec

import (
	"context"
	"encoding/json"
	"fmt"
	"os/exec"
	"strings"
	"time"

	"github.com/Microsoft/ApplicationInsights-Go/appinsights"
	"github.com/sourcegraph/checkup/types"
)

// Type should match the package name
const Type = "exec"

// Checker implements a Checker by running programs with os.Exec.
type Checker struct {
	// Name is the name of the endpoint.
	Name string `json:"name"`

	// Command is the main program entrypoint.
	Command string `json:"command"`

	// Arguments are individual program parameters.
	Arguments []string `json:"arguments,omitempty"`

	// ThresholdRTT is the maximum round trip time to
	// allow for a healthy endpoint. If non-zero and a
	// request takes longer than ThresholdRTT, the
	// endpoint will be considered unhealthy. Note that
	// this duration includes any in-between network
	// latency.
	ThresholdRTT time.Duration `json:"threshold_rtt,omitempty"`

	// MustContain is a string that the response body
	// must contain in order to be considered up.
	// NOTE: If set, the entire response body will
	// be consumed, which has the potential of using
	// lots of memory and slowing down checks if the
	// response body is large.
	MustContain string `json:"must_contain,omitempty"`

	// MustNotContain is a string that the response
	// body must NOT contain in order to be considered
	// up. If both MustContain and MustNotContain are
	// set, they are and-ed together. NOTE: If set,
	// the entire response body will be consumed, which
	// has the potential of using lots of memory and
	// slowing down checks if the response body is large.
	MustNotContain string `json:"must_not_contain,omitempty"`

	// Raise is a string that tells us if we should throw
	// a hard error ("error" - the default), or if we should
	// just mark something as degraded ("warn" or "warning").
	Raise string `json:"raise,omitempty"`

	// Attempts is how many requests the client will
	// make to the endpoint in a single check.
	Attempts int `json:"attempts,omitempty"`

	// AttemptSpacing spaces out each attempt in a check
	// by this duration to avoid hitting a remote too
	// quickly in succession. By default, no waiting
	// occurs between attempts.
	AttemptSpacing time.Duration `json:"attempt_spacing,omitempty"`

	// InstrumentationKey is a GUID used to send trackAvailability()
	// telemetry to Application Insights
	InstrumentationKey string `json:"instrumentation_key,omitempty"`

	// TestLocation identifies the test location sent
	// in Application Insights trackAvailability() events
	TestLocation string `json:"test_location,omitempty"`

	// InsecureTLS may be used to allow insecure TLS certificates
	// in HTTPS status checks
	InsecureTLS bool `json:"insecure_tls,omitempty"`

	// TelemetryClient is the appinsights.Client with which to
	// send Application Insights trackAvailability() events
	// Automatically created if InstrumentationKey is set.
	TelemetryClient appinsights.TelemetryClient `json:"-"`
}

// New creates a new Checker instance based on json config
func New(config json.RawMessage) (Checker, error) {
	var checker Checker
	err := json.Unmarshal(config, &checker)
	return checker, err
}

// Type returns the checker package name
func (Checker) Type() string {
	return Type
}

// Check performs checks using c according to its configuration.
// An error is only returned if there is a configuration error.
func (c Checker) Check() (types.Result, error) {
	if c.Attempts < 1 {
		c.Attempts = 1
	}
	if c.TestLocation == "" {
		c.TestLocation = "Checkup EXEC"
	}

	result := types.NewResult()
	result.Title = c.Name
	result.Endpoint = c.Command
	result.Times = c.doChecks()

	conclude := c.conclude(result)

	// Diagnostics message
	rtts := make([]string, len(conclude.Times))
	message := conclude.Notice
	if conclude.Degraded {
		for i := 0; i < c.Attempts; i++ {
			rtts[i] = conclude.Times[i].RTT.String()
		}
		message = fmt.Sprintf("%s - Number of attempts = %d (%s)", message, len(conclude.Times), strings.Join(rtts, " "))
	}

	if c.InstrumentationKey != "" {
		c.TelemetryClient = appinsights.NewTelemetryClient(c.InstrumentationKey)
		availability := appinsights.NewAvailabilityTelemetry(result.Title, result.Stats.Mean, result.Healthy)
		availability.RunLocation = c.TestLocation
		availability.Message = message
		// Id is used for correlation with the target service
		//availability.Id = requestId

		// Submit the telemetry
		c.TelemetryClient.Track(availability)
	}

	return conclude, nil
}

// doChecks executes command and returns each attempt.
func (c Checker) doChecks() types.Attempts {
	checks := make(types.Attempts, c.Attempts)
	for i := 0; i < c.Attempts; i++ {
		start := time.Now()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		command := exec.CommandContext(ctx, c.Command, c.Arguments...)
		output, err := command.CombinedOutput()

		checks[i].RTT = time.Since(start)

		if err != nil {
			stringify := func(s string) string {
				if strings.TrimSpace(s) == "" {
					return "empty"
				}
				return s
			}
			checks[i].Error = fmt.Sprintf("Error: %s\nOutput: %s\n", err.Error(), stringify(string(output)))
			continue
		}

		if err := c.checkDown(string(output)); err != nil {
			checks[i].Error = err.Error()
		}

		if c.AttemptSpacing > 0 {
			time.Sleep(c.AttemptSpacing)
		}
	}
	return checks
}

// conclude takes the data in result from the attempts and
// computes remaining values needed to fill out the result.
// It detects degraded (high-latency) responses and makes
// the conclusion about the result's status.
func (c Checker) conclude(result types.Result) types.Result {
	result.ThresholdRTT = c.ThresholdRTT

	warning := c.Raise == "warn" || c.Raise == "warning"

	// Check errors (down)
	for i := range result.Times {
		if result.Times[i].Error != "" {
			if warning {
				result.Notice = result.Times[i].Error
				result.Degraded = true
				return result
			}
			result.Down = true
			return result
		}
	}

	// Check round trip time (degraded)
	if c.ThresholdRTT > 0 {
		result.Stats = result.ComputeStats()
		if result.Stats.Median > c.ThresholdRTT {
			result.Notice = fmt.Sprintf("median round trip time exceeded threshold (%s)", c.ThresholdRTT)
			result.Degraded = true
			return result
		}
	}

	result.Healthy = true
	return result
}

// checkDown checks whether the endpoint is down based on resp and
// the configuration of c. It returns a non-nil error if down.
// Note that it does not check for degraded response.
func (c Checker) checkDown(body string) error {
	// Check response body
	if c.MustContain == "" && c.MustNotContain == "" {
		return nil
	}
	if c.MustContain != "" && !strings.Contains(body, c.MustContain) {
		return fmt.Errorf("response does not contain '%s'", c.MustContain)
	}
	if c.MustNotContain != "" && strings.Contains(body, c.MustNotContain) {
		return fmt.Errorf("response contains '%s'", c.MustNotContain)
	}
	return nil
}
