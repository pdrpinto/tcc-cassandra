package sensors

import (
	"context"
	"time"

	"github.com/gocql/gocql"
)

type RepositorioDeLeiturasDeSensores struct {
	SessaoDoCluster             *gocql.Session
	TempoLimiteDeEscrita        time.Duration
	TempoLimiteDeLeitura        time.Duration
	ConsistenciaPadraoDeEscrita gocql.Consistency
	ConsistenciaPadraoDeLeitura gocql.Consistency
}

func NovoRepositorioDeLeiturasDeSensores(sessao *gocql.Session, tempoEscrita, tempoLeitura time.Duration, consistEscrita, consistLeitura gocql.Consistency) *RepositorioDeLeiturasDeSensores {
	return &RepositorioDeLeiturasDeSensores{
		SessaoDoCluster:             sessao,
		TempoLimiteDeEscrita:        tempoEscrita,
		TempoLimiteDeLeitura:        tempoLeitura,
		ConsistenciaPadraoDeEscrita: consistEscrita,
		ConsistenciaPadraoDeLeitura: consistLeitura,
	}
}

func (r *RepositorioDeLeiturasDeSensores) GravarLeituraDeSensor(ctx context.Context, leitura LeituraDeSensor, consistencias ...gocql.Consistency) error {
	consistencia := r.ConsistenciaPadraoDeEscrita
	if len(consistencias) == 1 {
		consistencia = consistencias[0]
	}

	uuidTemporal := gocql.UUIDFromTime(leitura.InstanteDoEvento)
	dia := TruncarParaDiaUTC(leitura.DiaDeAgrupamento)

	ctxComTempoLimite, cancelar := context.WithTimeout(ctx, r.TempoLimiteDeEscrita)
	defer cancelar()

	q := r.SessaoDoCluster.Query(
		`INSERT INTO sensor_readings (sensor_id, day_bucket, ts, value, unit, status, tags)
         VALUES (?, ?, ?, ?, ?, ?, ?)`,
		leitura.IdentificadorDoSensor,
		dia,
		uuidTemporal,
		leitura.ValorMedido,
		leitura.UnidadeDeMedida,
		leitura.EstadoDaLeitura,
		leitura.AtributosAdicionais,
	).Consistency(consistencia).WithContext(ctxComTempoLimite)

	return q.Exec()
}

func (r *RepositorioDeLeiturasDeSensores) ConsultarUltimasLeiturasPorSensor(ctx context.Context, identificadorDoSensor string, diaDeAgrupamento time.Time, quantidade int, consistencias ...gocql.Consistency) ([]LeituraDeSensor, error) {
	consistencia := r.ConsistenciaPadraoDeLeitura
	if len(consistencias) == 1 {
		consistencia = consistencias[0]
	}

	ctxComTempoLimite, cancelar := context.WithTimeout(ctx, r.TempoLimiteDeLeitura)
	defer cancelar()

	dia := TruncarParaDiaUTC(diaDeAgrupamento)

	q := r.SessaoDoCluster.Query(
		`SELECT ts, value, unit, status, tags
         FROM sensor_readings
         WHERE sensor_id = ? AND day_bucket = ?
         LIMIT ?`,
		identificadorDoSensor, dia, quantidade,
	).Consistency(consistencia).WithContext(ctxComTempoLimite).PageSize(1000)

	iterador := q.Iter()
	defer iterador.Close()

	resultados := make([]LeituraDeSensor, 0, quantidade)
	var ts gocql.UUID
	var valor float64
	var unidade string
	var estado int16
	var tags map[string]string

	for iterador.Scan(&ts, &valor, &unidade, &estado, &tags) {
		resultados = append(resultados, LeituraDeSensor{
			IdentificadorDoSensor: identificadorDoSensor,
			DiaDeAgrupamento:      dia,
			InstanteDoEvento:      ts.Time(),
			ValorMedido:           valor,
			UnidadeDeMedida:       unidade,
			EstadoDaLeitura:       estado,
			AtributosAdicionais:   tags,
		})
	}

	if err := iterador.Close(); err != nil {
		return nil, err
	}

	return resultados, nil
}

func (r *RepositorioDeLeiturasDeSensores) ConsultarLeiturasPorIntervaloDeTempo(ctx context.Context, identificadorDoSensor string, diaDeAgrupamento, inicio, fim time.Time, quantidade int, consistencias ...gocql.Consistency) ([]LeituraDeSensor, error) {
	consistencia := r.ConsistenciaPadraoDeLeitura
	if len(consistencias) == 1 {
		consistencia = consistencias[0]
	}

	ctxComTempoLimite, cancelar := context.WithTimeout(ctx, r.TempoLimiteDeLeitura)
	defer cancelar()

	dia := TruncarParaDiaUTC(diaDeAgrupamento)
	uuidInicial := gocql.UUIDFromTime(inicio.UTC())
	uuidFinal := gocql.UUIDFromTime(fim.UTC())

	q := r.SessaoDoCluster.Query(
		`SELECT ts, value, unit, status, tags
         FROM sensor_readings
         WHERE sensor_id = ? AND day_bucket = ? AND ts >= ? AND ts <= ?
         LIMIT ?`,
		identificadorDoSensor, dia, uuidInicial, uuidFinal, quantidade,
	).Consistency(consistencia).WithContext(ctxComTempoLimite).PageSize(1000)

	iterador := q.Iter()
	defer iterador.Close()

	resultados := make([]LeituraDeSensor, 0, quantidade)
	var ts gocql.UUID
	var valor float64
	var unidade string
	var estado int16
	var tags map[string]string

	for iterador.Scan(&ts, &valor, &unidade, &estado, &tags) {
		resultados = append(resultados, LeituraDeSensor{
			IdentificadorDoSensor: identificadorDoSensor,
			DiaDeAgrupamento:      dia,
			InstanteDoEvento:      ts.Time(),
			ValorMedido:           valor,
			UnidadeDeMedida:       unidade,
			EstadoDaLeitura:       estado,
			AtributosAdicionais:   tags,
		})
	}
	if err := iterador.Close(); err != nil {
		return nil, err
	}
	return resultados, nil
}
