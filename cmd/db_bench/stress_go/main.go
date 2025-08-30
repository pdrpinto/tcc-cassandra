package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/pdrpinto/tcc-cassandra/internal/stress/adaptadores"
	"github.com/pdrpinto/tcc-cassandra/internal/stress/aplicacao"
)

func converteConsistencia(texto string) gocql.Consistency {
	switch strings.ToUpper(strings.TrimSpace(texto)) {
	case "ONE":
		return gocql.One
	case "TWO":
		return gocql.Two
	case "THREE":
		return gocql.Three
	case "QUORUM":
		return gocql.Quorum
	case "ALL":
		return gocql.All
	case "LOCAL_ONE":
		return gocql.LocalOne
	case "LOCAL_QUORUM":
		return gocql.LocalQuorum
	default:
		return gocql.Quorum
	}
}

func main() {
	var (
		hostsPadrao       = valorOu("CASSANDRA_HOSTS", "cassandra1:9042,cassandra2:9042,cassandra3:9042")
		keyspacePadrao    = valorOu("CASSANDRA_KEYSPACE", "tcc")
		consistPadrao     = valorOu("CONSISTENCY", "QUORUM")
		duracaoPadrao     = valorOu("DURATION", "0s") // 0 = contínuo
		rpsPadrao         = valorOuInteiro("RPS", 1000)
		concPadrao        = valorOuInteiro("CONC", runtime.NumCPU()*2)
		sensoresPadrao    = valorOuInteiro("SENSORS", 5000)
		metricsAddrPadrao = valorOu("METRICS_ADDR", ":9100")
		progressoPadrao   = valorOu("PROGRESS_INTERVAL", "5s")
	)

	var (
		parametroListaDeHostsCassandra         = flag.String("hosts", hostsPadrao, "Hosts do Cassandra separados por vírgula")
		parametroNomeDoKeyspace                = flag.String("keyspace", keyspacePadrao, "Keyspace a utilizar")
		parametroNivelDeConsistencia           = flag.String("consistency", consistPadrao, "Nível de consistência")
		parametroDuracaoTotalDoTeste           = flag.Duration("dur", deveParsearDuracao(duracaoPadrao), "Duração total do teste")
		parametroTaxaDeRequisicoesPorSegundo   = flag.Int("rps", rpsPadrao, "RPS desejado")
		parametroGrauDeConcorrencia            = flag.Int("conc", concPadrao, "Concorrência")
		parametroQuantidadeDeSensoresDistintos = flag.Int("sensors", sensoresPadrao, "Sensores distintos")
		parametroEnderecoDeMetricas            = flag.String("metrics", metricsAddrPadrao, "Endereço :porta de métricas Prometheus")
		parametroIntervaloDeLogs               = flag.Duration("progress", deveParsearDuracao(progressoPadrao), "Intervalo para logs de progresso (0=desliga)")
	)
	flag.Parse()

	// Conexão Cassandra
	cluster := gocql.NewCluster(dividirHosts(*parametroListaDeHostsCassandra)...)
	cluster.Keyspace = *parametroNomeDoKeyspace
	cluster.ProtoVersion = 4
	cluster.Timeout = 5 * time.Second
	cluster.Consistency = converteConsistencia(*parametroNivelDeConsistencia)
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(gocql.RoundRobinHostPolicy())
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 1}

	sessao, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	defer sessao.Close()

	// Adaptadores
	adaptadorDeMetricas := adaptadores.NovoRegistradorDeMetricas()
	adaptadores.IniciarServidorDeMetricas(*parametroEnderecoDeMetricas)
	repositorioDeEscrita := adaptadores.NovoRepositorioDeEscritaCassandra(sessao, converteConsistencia(*parametroNivelDeConsistencia), 5*time.Second)

	// Serviço de aplicação (orquestra a carga)
	servico := aplicacao.ServicoDeStress{Persistencia: repositorioDeEscrita, Metricas: adaptadorDeMetricas}
	cfg := aplicacao.ConfiguracaoDoTesteDeStress{
		ListaDeHostsCassandra:         dividirHosts(*parametroListaDeHostsCassandra),
		NomeDoKeyspace:                *parametroNomeDoKeyspace,
		NivelDeConsistenciaTexto:      strings.ToUpper(*parametroNivelDeConsistencia),
		DuracaoTotalDoTeste:           *parametroDuracaoTotalDoTeste,
		TaxaDeRequisicoesPorSegundo:   *parametroTaxaDeRequisicoesPorSegundo,
		GrauDeConcorrencia:            *parametroGrauDeConcorrencia,
		QuantidadeDeSensoresDistintos: *parametroQuantidadeDeSensoresDistintos,
		IntervaloDeLogDeProgresso:     *parametroIntervaloDeLogs,
	}

	total, ok, dur := servico.Executar(context.Background(), cfg)
	fmt.Printf("go-stress concluido: total=%d ok=%d duracao_ms=%d cons=%s\n", total, ok, dur.Milliseconds(), cfg.NivelDeConsistenciaTexto)
}

func dividirHosts(lista string) []string {
	partes := strings.Split(lista, ",")
	saida := make([]string, 0, len(partes))
	for _, p := range partes {
		p = strings.TrimSpace(p)
		if p != "" {
			saida = append(saida, p)
		}
	}
	return saida
}

func valorOu(chave, padrao string) string {
	if v := strings.TrimSpace(os.Getenv(chave)); v != "" {
		return v
	}
	return padrao
}

func valorOuInteiro(chave string, padrao int) int {
	if v := strings.TrimSpace(os.Getenv(chave)); v != "" {
		var n int
		if _, err := fmt.Sscanf(v, "%d", &n); err == nil && n > 0 {
			return n
		}
	}
	return padrao
}

func deveParsearDuracao(s string) time.Duration {
	d, err := time.ParseDuration(s)
	if err != nil {
		return 30 * time.Second
	}
	return d
}
