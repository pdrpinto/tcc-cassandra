package main

import (
	"context"
	"flag"
	"fmt"
	"strings"
	"time"

	injAdapt "github.com/pdrpinto/tcc-cassandra/internal/falhas/adaptadores"
	injApp "github.com/pdrpinto/tcc-cassandra/internal/falhas/aplicacao"
	injPorts "github.com/pdrpinto/tcc-cassandra/internal/falhas/portas"
)

func main() {
	var (
		parametroCenario    = flag.String("cenario", "derrubar1", "Cenario: derrubar1|derrubar2|particao|custom")
		parametroContainers = flag.String("containers", "cassandra2", "Lista de containers separados por vírgula (para custom)")
		parametroRede       = flag.String("rede", "tcc-net", "Nome da rede Docker (para partição)")
	)
	flag.Parse()

	orq := injAdapt.NovoOrquestradorDeFalhasDockerCLI()
	servico := injApp.ServicoDeInjecaoDeFalhas{Orquestrador: orq}
	ctx := context.Background()

	var plano []injPorts.EtapaDoPlano
	switch *parametroCenario {
	case "derrubar1":
		plano = []injPorts.EtapaDoPlano{
			{MomentoRelativoSegundos: 5, Acao: "parar", NomeDoContainer: "cassandra2", TimeoutSegundos: 10},
			{MomentoRelativoSegundos: 60, Acao: "continuar", NomeDoContainer: "cassandra2"}, // no effect after stop, illustrative
		}
	case "derrubar2":
		plano = []injPorts.EtapaDoPlano{
			{MomentoRelativoSegundos: 5, Acao: "parar", NomeDoContainer: "cassandra2", TimeoutSegundos: 10},
			{MomentoRelativoSegundos: 10, Acao: "parar", NomeDoContainer: "cassandra3", TimeoutSegundos: 10},
		}
	case "particao":
		plano = []injPorts.EtapaDoPlano{
			{MomentoRelativoSegundos: 5, Acao: "desconectar", NomeDoContainer: "cassandra2", NomeDaRede: *parametroRede},
			{MomentoRelativoSegundos: 60, Acao: "reconectar", NomeDoContainer: "cassandra2", NomeDaRede: *parametroRede},
		}
	case "custom":
		ids := strings.Split(*parametroContainers, ",")
		momento := 5
		for _, id := range ids {
			id = strings.TrimSpace(id)
			if id == "" {
				continue
			}
			plano = append(plano, injPorts.EtapaDoPlano{MomentoRelativoSegundos: momento, Acao: "parar", NomeDoContainer: id, TimeoutSegundos: 10})
			momento += 5
		}
	default:
		fmt.Println("Cenário inválido")
		return
	}

	fmt.Printf("Executando plano de falhas (%d etapas)\n", len(plano))
	inicio := time.Now()
	if err := servico.ExecutarPlano(ctx, plano); err != nil {
		fmt.Printf("Erro no plano: %v\n", err)
		return
	}
	fmt.Printf("Plano concluído em %s\n", time.Since(inicio))
}
