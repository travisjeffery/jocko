package mock

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/travisjeffery/jocko"
)

func NewMetrics() *jocko.Metrics {
	return &jocko.Metrics{
		RequestsHandled: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "requests_handled",
			Help: "Number of requests handled by the server.",
		}),
	}
}
