package prometheus

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/travisjeffery/jocko"
)

func NewMetrics() *jocko.Metrics {
	m := &jocko.Metrics{
		RequestsHandled: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "requests_handled",
			Help: "Number of requests handled by the server.",
		}),
	}
	prometheus.DefaultRegisterer.MustRegister(m.RequestsHandled)
	return m
}
