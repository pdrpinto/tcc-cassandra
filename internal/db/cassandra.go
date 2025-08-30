package db

import (
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/pdrpinto/tcc-cassandra/internal/config"
)

type ClienteDeBancoCassandra struct {
	SessaoDoCluster             *gocql.Session
	ConsistenciaPadraoDeEscrita gocql.Consistency
	ConsistenciaPadraoDeLeitura gocql.Consistency
}

func CriarClienteDeBancoCassandra(cfg config.ConfiguracoesDeConexaoComCassandra) (*ClienteDeBancoCassandra, error) {
	consistW, err := ConverterTextoParaNivelDeConsistencia(cfg.NivelDeConsistenciaDeEscrita)
	if err != nil {
		return nil, err
	}

	consistR, err := ConverterTextoParaNivelDeConsistencia(cfg.NivelDeConsistenciaDeLeitura)
	if err != nil {
		return nil, err
	}

	cluster := gocql.NewCluster(cfg.EnderecoDosNodosCassandra...)
	cluster.Keyspace = cfg.NomeDoKeyspace
	cluster.ProtoVersion = 4
	cluster.Timeout = 5 * time.Second
	cluster.ConnectTimeout = 10 * time.Second

	// Politicas: boa distribuicao e resiliencia a falhas transitorias
	rr := gocql.RoundRobinHostPolicy()
	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(rr)
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 1}
	cluster.ReconnectionPolicy = &gocql.ExponentialReconnectionPolicy{
		InitialInterval: 500 * time.Millisecond,
		MaxInterval:     10 * time.Second,
	}

	// Consistencia default (override por operacoes nas queries)
	cluster.Consistency = consistW

	sessao, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	return &ClienteDeBancoCassandra{
		SessaoDoCluster:             sessao,
		ConsistenciaPadraoDeEscrita: consistW,
		ConsistenciaPadraoDeLeitura: consistR,
	}, nil
}

func (c *ClienteDeBancoCassandra) FecharConexaoComCassandra() {
	if c.SessaoDoCluster != nil {
		c.SessaoDoCluster.Close()
	}
}

func ConverterTextoParaNivelDeConsistencia(valor string) (gocql.Consistency, error) {
	switch valor {
	case "ONE":
		return gocql.One, nil
	case "TWO":
		return gocql.Two, nil
	case "THREE":
		return gocql.Three, nil
	case "QUORUM":
		return gocql.Quorum, nil
	case "ALL":
		return gocql.All, nil
	case "LOCAL_QUORUM":
		return gocql.LocalQuorum, nil
	case "LOCAL_ONE":
		return gocql.LocalOne, nil
	default:
		return gocql.Any, fmt.Errorf("nivel de consistencia desconhecido: %s", valor)
	}
}
