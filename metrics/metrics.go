package metrics

import (
	"context"
	"time"

	"go.opencensus.io/stats"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/tag"
)

// Distribution
var defaultMillisecondsDistribution = view.Distribution(0.01, 0.05, 0.1, 0.3, 0.6, 0.8, 1, 2, 3, 4, 5, 6, 8, 10, 13, 16, 20, 25, 30, 40, 50, 65, 80, 100, 130, 160, 200, 250, 300, 400, 500, 650, 800, 1000, 2000, 3000, 4000, 5000, 7500, 10000, 20000, 50000, 100_000, 250_000, 500_000, 1000_000)

// Tags
var (
	Version, _ = tag.NewKey("version")
	Commit, _  = tag.NewKey("commit")

	Endpoint, _ = tag.NewKey("endpoint")
)

// Measures
var (
	Info               = stats.Int64("info", "Arbitrary counter to tag rtb info to", stats.UnitDimensionless)
	APIRequestDuration = stats.Float64("api/request_duration_ms", "Duration of API requests", stats.UnitMilliseconds)
)

// Views
var (
	InfoView = &view.View{
		Name:        "info",
		Description: "rbot information",
		Measure:     Info,
		Aggregation: view.LastValue(),
		TagKeys:     []tag.Key{Version, Commit},
	}
	APIRequestDurationView = &view.View{
		Measure:     APIRequestDuration,
		Aggregation: defaultMillisecondsDistribution,
		TagKeys:     []tag.Key{Endpoint},
	}
)

var Views = []*view.View{
	InfoView,
	APIRequestDurationView,
}

// SinceInMilliseconds returns the duration of time since the provide time as a float64.
func SinceInMilliseconds(startTime time.Time) float64 {
	return float64(time.Since(startTime).Nanoseconds()) / 1e6
}

// Timer is a function stopwatch, calling it starts the timer,
// calling the returned function will record the duration.
func Timer(ctx context.Context, m *stats.Float64Measure) func() time.Duration {
	start := time.Now()
	return func() time.Duration {
		stats.Record(ctx, m.M(SinceInMilliseconds(start)))
		return time.Since(start)
	}
}
