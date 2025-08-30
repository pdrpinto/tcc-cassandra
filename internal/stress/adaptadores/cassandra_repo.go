package adaptadores

import (
	"context"
	"time"

	"github.com/gocql/gocql"
	"github.com/pdrpinto/tcc-cassandra/internal/stress/portas"
)

type RepositorioDeEscritaCassandra struct {
	SessaoDoClusterCassandra *gocql.Session
	NivelDeConsistencia      gocql.Consistency
	TempoLimitePorOperacao   time.Duration
}

func NovoRepositorioDeEscritaCassandra(sessao *gocql.Session, consist gocql.Consistency, timeout time.Duration) *RepositorioDeEscritaCassandra {
	return &RepositorioDeEscritaCassandra{
		SessaoDoClusterCassandra: sessao,
		NivelDeConsistencia:      consist,
		TempoLimitePorOperacao:   timeout,
	}
}

func (r *RepositorioDeEscritaCassandra) GravarLeitura(ctx context.Context, leitura portas.LeituraDeSensor) error {
	ctxComTempo, cancelar := context.WithTimeout(ctx, r.TempoLimitePorOperacao)
	defer cancelar()
	consulta := r.SessaoDoClusterCassandra.Query(
		`INSERT INTO sensor_readings (sensor_id, day_bucket, ts, value, unit, status, tags) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		leitura.IdentificadorDoSensor,
		leitura.DiaDeAgrupamento,
		gocql.UUIDFromTime(leitura.InstanteDoEvento),
		leitura.ValorMedido,
		leitura.UnidadeDeMedida,
		leitura.EstadoDaLeitura,
		leitura.AtributosAdicionais,
	).Consistency(r.NivelDeConsistencia).WithContext(ctxComTempo)
	return consulta.Exec()
}
