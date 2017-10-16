package server

import "github.com/prometheus/client_golang/prometheus"

type serverMetrics struct {
	requestsHandled prometheus.Counter
}

func newServerMetrics(r prometheus.Registerer) *serverMetrics {
	m := &serverMetrics{
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
