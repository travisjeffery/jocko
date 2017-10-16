package server

import "github.com/prometheus/client_golang/prometheus"

type metrics struct {
	requestsHandled prometheus.Counter
}

func newMetrics(r prometheus.Registerer) *metrics {
	m := &metrics{
		requestsHandled: prometheus.NewCounter(prometheus.CounterOpts{
			Name: "requests_handled",
			Help: "Number of requests handled by the server.",
		}),
	}
	if r != nil {
		r.MustRegister(
			m.requestsHandled,
		)
	}
	return m
}
