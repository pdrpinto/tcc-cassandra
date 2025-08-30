package sensors

import (
	"time"
)

type LeituraDeSensor struct {
	IdentificadorDoSensor string
	DiaDeAgrupamento      time.Time
	InstanteDoEvento      time.Time
	ValorMedido           float64
	UnidadeDeMedida       string
	EstadoDaLeitura       int16
	AtributosAdicionais   map[string]string
}

// Trunca o tempo para o inicio do dia em UTC para bucket diario
func TruncarParaDiaUTC(t time.Time) time.Time {
	utc := t.UTC()
	return time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
}
