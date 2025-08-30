package metrics

import (
	"net/http"
	"strconv"
	"time"

	"github.com/gocql/gocql"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	LatenciaOperacaoMs = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "latencias_operacao_ms",
			Help:    "Latência das operações em milissegundos",
			Buckets: []float64{1, 5, 10, 20, 50, 100, 200, 500, 1000, 2000, 5000},
		},
		[]string{"rota", "consistencia"},
	)
	ErrosPorRota = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "errors_por_rota_total",
			Help: "total de erros por rota, rotulado pelo motivo",
		},
		[]string{"rota", "motivo"},
	)
)

func MustRegister() {
	prometheus.MustRegister(LatenciaOperacaoMs, ErrosPorRota)
}

func HandlerMetrics() http.Handler {
	return promhttp.Handler()
}

func RegistrarLatencia(rota string, cons gocql.Consistency, dur time.Duration) {
	LatenciaOperacaoMs.WithLabelValues(rota, consistenciaParaLabel(cons)).Observe(float64(dur.Milliseconds()))
}

func RegistrarErro(rota, motivo string) {
	ErrosPorRota.WithLabelValues(rota, motivo).Inc()
}

func consistenciaParaLabel(c gocql.Consistency) string {
	switch c {
	case gocql.One:
		return "ONE"
	case gocql.Two:
		return "TWO"
	case gocql.Three:
		return "THREE"
	case gocql.Quorum:
		return "QUORUM"
	case gocql.All:
		return "ALL"
	case gocql.LocalQuorum:
		return "LOCAL_QUORUM"
	case gocql.LocalOne:
		return "LOCAL_ONE"
	default:
		return "UNKNOWN_" + strconv.Itoa(int(c))
	}
}
