package portas

import (
	"context"
	"time"
)

// Dados de leitura a serem gravados durante o teste de stress.
type LeituraDeSensor struct {
	IdentificadorDoSensor string
	DiaDeAgrupamento      time.Time
	InstanteDoEvento      time.Time
	ValorMedido           float64
	UnidadeDeMedida       string
	EstadoDaLeitura       int16
	AtributosAdicionais   map[string]string
}

// Porta (interface) para persistencia de leituras (adaptador de banco implementa).
type PortaDeEscrita interface {
	GravarLeitura(ctx context.Context, leitura LeituraDeSensor) error
}

// Porta (interface) para registro de metricas.
type PortaDeMetricas interface {
	RegistrarLatenciaEmMs(rotuloConsistencia string, duracao time.Duration)
	RegistrarErro(motivo string)
}
