package main

import (
	"log"
	"net/http"

	"github.com/pdrpinto/tcc-cassandra/internal/config"
	"github.com/pdrpinto/tcc-cassandra/internal/db"
	"github.com/pdrpinto/tcc-cassandra/internal/httpingestor"
	"github.com/pdrpinto/tcc-cassandra/internal/metrics"
	"github.com/pdrpinto/tcc-cassandra/internal/sensors"
)

func main() {
	configuracoes := config.CarregarConfiguracoesAPartirDeVariaveisDeAmbiente()

	cliente, err := db.CriarClienteDeBancoCassandra(configuracoes)
	if err != nil {
		log.Fatalf("Falha ao conectar ao Cassandra: %v", err)
	}
	defer cliente.FecharConexaoComCassandra()

	repositorio := sensors.NovoRepositorioDeLeiturasDeSensores(
		cliente.SessaoDoCluster,
		configuracoes.TempoLimiteDeEscrita,
		configuracoes.TempoLimiteDeLeitura,
		cliente.ConsistenciaPadraoDeEscrita,
		cliente.ConsistenciaPadraoDeLeitura,
	)

	handler := httpingestor.NovoRoteadorDeIngestao(httpingestor.DependenciasDoHandler{
		Repositorio: repositorio,
	})

	// Metrics
	metrics.MustRegister()
	mux := http.NewServeMux()
	mux.Handle("/metrics", metrics.HandlerMetrics())
	mux.Handle("/", handler)

	endereco := ":8080"
	log.Printf("Servidor de ingestão escutando em %s", endereco)
	if err := http.ListenAndServe(endereco, mux); err != nil {
		log.Fatalf("Erro no servidor HTTP: %v", err)
	}
}
