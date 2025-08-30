package portas

import "context"

// Porta para orquestração de falhas no ambiente (ex.: Docker Desktop)
type PortaDeOrquestracaoDeFalhas interface {
	PausarNo(ctx context.Context, nomeDoContainer string) error
	ContinuarNo(ctx context.Context, nomeDoContainer string) error
	PararNo(ctx context.Context, nomeDoContainer string, timeoutSegundos int) error
	DesconectarNoDaRede(ctx context.Context, nomeDoContainer string, nomeDaRede string, forcar bool) error
	ReconectarNoARede(ctx context.Context, nomeDoContainer string, nomeDaRede string) error
}

// Plano de injeção de falhas com etapas sequenciadas no tempo.
type EtapaDoPlano struct {
	MomentoRelativoSegundos int
	Acao                    string // "pausar", "continuar", "parar", "desconectar", "reconectar"
	NomeDoContainer         string
	NomeDaRede              string // usado para desconectar/reconectar
	TimeoutSegundos         int    // usado para parar
}
