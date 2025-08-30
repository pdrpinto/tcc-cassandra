package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

type ConfiguracoesDeConexaoComCassandra struct {
	EnderecoDosNodosCassandra    []string
	NomeDoKeyspace               string
	NivelDeConsistenciaDeEscrita string // one, quorom, all, etc
	NivelDeConsistenciaDeLeitura string // one, quorom, all, etc
	TempoLimiteDeEscrita         time.Duration
	TempoLimiteDeLeitura         time.Duration
	NomeDoDataCenterLocal        string
}

func CarregarConfiguracoesAPartirDeVariaveisDeAmbiente() ConfiguracoesDeConexaoComCassandra {
	enderecos := valorOuPadrao(os.Getenv("CASSANDRA_HOSTS"), "cassandra1:9042")
	keyspace := valorOuPadrao(os.Getenv("CASSANDRA_KEYSPACE"), "tcc")
	consistEscrita := strings.ToUpper(strings.TrimSpace(valorOuPadrao(os.Getenv("CONSISTENCY_WRITE"), "QUORUM")))
	consistLeitura := strings.ToUpper(strings.TrimSpace(valorOuPadrao(os.Getenv("CONSISTENCY_READ"), "QUORUM")))
	tempoLimiteEscrita := converterTextoParaDuracaoEmMs(os.Getenv("WRITE_TIMEOUT_MS"), 1500)
	tempoLimiteLeitura := converterTextoParaDuracaoEmMs(os.Getenv("READ_TIMEOUT_MS"), 1500)
	dataCenter := valorOuPadrao(os.Getenv("LOCAL_DC"), "dc1")

	return ConfiguracoesDeConexaoComCassandra{
		EnderecoDosNodosCassandra:    dividirHosts(enderecos),
		NomeDoKeyspace:               keyspace,
		NivelDeConsistenciaDeEscrita: consistEscrita,
		NivelDeConsistenciaDeLeitura: consistLeitura,
		TempoLimiteDeEscrita:         tempoLimiteEscrita,
		TempoLimiteDeLeitura:         tempoLimiteLeitura,
		NomeDoDataCenterLocal:        dataCenter,
	}
}

func valorOuPadrao(valor string, padrao string) string {
	if strings.TrimSpace(valor) == "" {
		return padrao
	}
	return valor
}

func dividirHosts(lista string) []string {
	partes := strings.Split(strings.TrimSpace(lista), ",")
	res := make([]string, 0, len(partes))
	for _, p := range partes {
		p = strings.TrimSpace(p)
		if p != "" {
			res = append(res, p)
		}
	}
	return res
}

func converterTextoParaDuracaoEmMs(valor string, padraoMs int) time.Duration {
	if strings.TrimSpace(valor) == "" {
		return time.Duration(padraoMs) * time.Millisecond
	}

	n, err := strconv.Atoi(valor)
	if err != nil || n <= 0 {
		return time.Duration(padraoMs) * time.Millisecond
	}
	return time.Duration(n) * time.Millisecond
}
