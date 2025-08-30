package aplicacao

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/pdrpinto/tcc-cassandra/internal/stress/portas"
)

type ConfiguracaoDoTesteDeStress struct {
	ListaDeHostsCassandra         []string
	NomeDoKeyspace                string
	NivelDeConsistenciaTexto      string
	DuracaoTotalDoTeste           time.Duration
	TaxaDeRequisicoesPorSegundo   int
	GrauDeConcorrencia            int
	QuantidadeDeSensoresDistintos int
	IntervaloDeLogDeProgresso     time.Duration // 0 desativa logs periódicos
}

type ServicoDeStress struct {
	Persistencia portas.PortaDeEscrita
	Metricas     portas.PortaDeMetricas
}

func (s *ServicoDeStress) Executar(ctx context.Context, cfg ConfiguracaoDoTesteDeStress) (total int64, ok int64, duracao time.Duration) {
	var prazo context.Context
	var cancelar context.CancelFunc
	if cfg.DuracaoTotalDoTeste > 0 {
		prazo, cancelar = context.WithTimeout(ctx, cfg.DuracaoTotalDoTeste)
	} else {
		prazo, cancelar = context.WithCancel(ctx) // modo contínuo
	}
	defer cancelar()

	var totalContador, okContador atomic.Int64
	tique := time.NewTicker(time.Second / time.Duration(cfg.TaxaDeRequisicoesPorSegundo))
	defer tique.Stop()

	sem := make(chan struct{}, cfg.GrauDeConcorrencia)
	grupo := &sync.WaitGroup{}

	rotuloCons := cfg.NivelDeConsistenciaTexto
	inicio := time.Now()

	// Contadores locais de erro para log periódico
	var cntTimeout, cntUnavailable, cntOverloaded, cntOther atomic.Int64

	// Log periódico de progresso (ops/s e erros)
	var tiqueProgresso *time.Ticker
	if cfg.IntervaloDeLogDeProgresso > 0 {
		tiqueProgresso = time.NewTicker(cfg.IntervaloDeLogDeProgresso)
		defer tiqueProgresso.Stop()
	}

	var ultimoTotal int64

	for {
		select {
		case <-prazo.Done():
			goto FIM
		case <-tiqueProgresso.C:
			atual := totalContador.Load()
			delta := atual - ultimoTotal
			ultimoTotal = atual
			janela := cfg.IntervaloDeLogDeProgresso
			if janela <= 0 {
				janela = 1 * time.Second
			}
			opss := float64(delta) / janela.Seconds()
			fmt.Printf("[stress] progresso: total=%d ok=%d ops/s=%.0f erros{timeout=%d unavailable=%d overloaded=%d other=%d}\n",
				atual, okContador.Load(), opss, cntTimeout.Load(), cntUnavailable.Load(), cntOverloaded.Load(), cntOther.Load())
		case <-tique.C:
			sem <- struct{}{}
			grupo.Add(1)
			go func() {
				defer grupo.Done()
				defer func() { <-sem }()
				id := fmt.Sprintf("stress-%d", rand.Intn(cfg.QuantidadeDeSensoresDistintos))
				agora := time.Now().UTC()
				dia := time.Date(agora.Year(), agora.Month(), agora.Day(), 0, 0, 0, 0, time.UTC)
				leitura := portas.LeituraDeSensor{
					IdentificadorDoSensor: id,
					DiaDeAgrupamento:      dia,
					InstanteDoEvento:      agora,
					ValorMedido:           rand.Float64()*100 + 1,
					UnidadeDeMedida:       "C",
					EstadoDaLeitura:       0,
					AtributosAdicionais:   map[string]string{"src": "go-stress", "site": "LAB", "tipo": "temperatura"},
				}
				t0 := time.Now()
				if err := s.Persistencia.GravarLeitura(context.Background(), leitura); err != nil {
					motivo := classificarErro(err)
					s.Metricas.RegistrarErro(motivo)
					switch motivo {
					case "timeout":
						cntTimeout.Add(1)
					case "unavailable":
						cntUnavailable.Add(1)
					case "overloaded":
						cntOverloaded.Add(1)
					default:
						cntOther.Add(1)
					}
				} else {
					okContador.Add(1)
					s.Metricas.RegistrarLatenciaEmMs(rotuloCons, time.Since(t0))
				}
				totalContador.Add(1)
			}()
		}
	}
FIM:
	grupo.Wait()
	return totalContador.Load(), okContador.Load(), time.Since(inicio)
}

func classificarErro(err error) string {
	msg := err.Error()
	switch {
	case containsFold(msg, "timeout"):
		return "timeout"
	case containsFold(msg, "unavailable"):
		return "unavailable"
	case containsFold(msg, "overloaded"):
		return "overloaded"
	default:
		return "other"
	}
}

func containsFold(s, substr string) bool { return stringIndexFold(s, substr) >= 0 }

func stringIndexFold(s, substr string) int {
	return indexFold(s, substr)
}

func indexFold(s, sep string) int {
	ls, lsep := len(s), len(sep)
	for i := 0; i+lsep <= ls; i++ {
		if equalFold(s[i:i+lsep], sep) {
			return i
		}
	}
	return -1
}

func equalFold(a, b string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := 0; i < len(a); i++ {
		ca, cb := a[i], b[i]
		if 'A' <= ca && ca <= 'Z' {
			ca += 'a' - 'A'
		}
		if 'A' <= cb && cb <= 'Z' {
			cb += 'a' - 'A'
		}
		if ca != cb {
			return false
		}
	}
	return true
}
