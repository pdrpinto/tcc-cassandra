package aplicacao

import (
	"context"
	"fmt"
	"sort"
	"time"

	p "github.com/pdrpinto/tcc-cassandra/internal/falhas/portas"
)

type ServicoDeInjecaoDeFalhas struct {
	Orquestrador p.PortaDeOrquestracaoDeFalhas
}

func (s *ServicoDeInjecaoDeFalhas) ExecutarPlano(ctx context.Context, plano []p.EtapaDoPlano) error {
	// Ordena por MomentoRelativoSegundos para execução determinística
	etapas := append([]p.EtapaDoPlano(nil), plano...)
	sort.Slice(etapas, func(i, j int) bool { return etapas[i].MomentoRelativoSegundos < etapas[j].MomentoRelativoSegundos })
	inicio := time.Now()
	for _, e := range etapas {
		alvo := inicio.Add(time.Duration(e.MomentoRelativoSegundos) * time.Second)
		if atraso := time.Until(alvo); atraso > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(atraso):
			}
		}
		var err error
		switch e.Acao {
		case "pausar":
			err = s.Orquestrador.PausarNo(ctx, e.NomeDoContainer)
		case "continuar":
			err = s.Orquestrador.ContinuarNo(ctx, e.NomeDoContainer)
		case "parar":
			err = s.Orquestrador.PararNo(ctx, e.NomeDoContainer, e.TimeoutSegundos)
		case "desconectar":
			err = s.Orquestrador.DesconectarNoDaRede(ctx, e.NomeDoContainer, e.NomeDaRede, true)
		case "reconectar":
			err = s.Orquestrador.ReconectarNoARede(ctx, e.NomeDoContainer, e.NomeDaRede)
		default:
			err = fmt.Errorf("acao desconhecida: %s", e.Acao)
		}
		if err != nil {
			return fmt.Errorf("falha na etapa %+v: %w", e, err)
		}
	}
	return nil
}
