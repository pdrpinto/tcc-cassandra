package adaptadores

import (
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type RegistradorDeMetricasPrometheus struct {
	HistLatenciaMs *prometheus.HistogramVec
	CntErros       *prometheus.CounterVec
}

func NovoRegistradorDeMetricas() *RegistradorDeMetricasPrometheus {
	h := prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name:    "stress_write_latency_ms",
		Help:    "Latencia de escrita em ms",
		Buckets: []float64{1, 5, 10, 20, 50, 100, 200, 500, 1000, 2000},
	}, []string{"consistencia"})
	c := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "stress_errors_total",
		Help: "Erros totais por motivo",
	}, []string{"motivo"})
	prometheus.MustRegister(h, c)
	return &RegistradorDeMetricasPrometheus{HistLatenciaMs: h, CntErros: c}
}

func (r *RegistradorDeMetricasPrometheus) RegistrarLatenciaEmMs(rotuloConsistencia string, dur time.Duration) {
	r.HistLatenciaMs.WithLabelValues(rotuloConsistencia).Observe(float64(dur.Milliseconds()))
}

func (r *RegistradorDeMetricasPrometheus) RegistrarErro(motivo string) {
	r.CntErros.WithLabelValues(motivo).Inc()
}

func IniciarServidorDeMetricas(endereco string) {
	http.Handle("/metrics", promhttp.Handler())
	go http.ListenAndServe(endereco, nil)
}
