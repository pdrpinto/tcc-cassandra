package httpingestor

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gocql/gocql"

	"github.com/pdrpinto/tcc-cassandra/internal/db"
	"github.com/pdrpinto/tcc-cassandra/internal/sensors"
)

type DependenciasDoHandler struct {
	Repositorio *sensors.RepositorioDeLeiturasDeSensores
}

type RequisicaoDeIngestao struct {
	IdentificadorDoSensor   string            `json:"identificador_do_sensor"`
	InstanteDoEventoISO8601 string            `json:"instante_do_evento_iso8601"`
	ValorMedido             float64           `json:"valor_medido"`
	UnidadeDeMedida         string            `json:"unidade_de_medida,omitempty"`
	EstadoDaLeitura         int16             `json:"estado_da_leitura,omitempty"`
	AtributosAdicionais     map[string]string `json:"atributos_adicionais,omitempty"`
}

type RespostaDeIngestao struct {
	Sucesso          bool   `json:"sucesso"`
	DuracaoEmMs      int64  `json:"duracao_ms"`
	Erro             string `json:"erro,omitempty"`
	QuantidadeAceita int    `json:"quantidade_aceita,omitempty"`
	QuantidadeFalha  int    `json:"quantidade_falha,omitempty"`
}

// Constrói e retorna um http.Handler com todas as rotas do ingestor.
func NovoRoteadorDeIngestao(dep DependenciasDoHandler) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/ingest", dep.manipuladorDeRequisicoesDeIngestao)
	mux.HandleFunc("/ingest/lote", dep.manipuladorDeRequisicoesDeIngestaoEmLote)

	RegistrarRotasDeLeitura(mux, dep)
	return mux
}

func (d DependenciasDoHandler) manipuladorDeRequisicoesDeIngestao(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Somente POST", http.StatusMethodNotAllowed)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 1<<20) // Limita a 1MB

	var req RequisicaoDeIngestao
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, "json invalido: "+err.Error(), http.StatusBadRequest)
		return
	}

	instante, err := time.Parse(time.RFC3339, req.InstanteDoEventoISO8601)
	if err != nil {
		http.Error(w, "instante_do_evento_iso8601 invalido (use rfc3339/utc): "+err.Error(), http.StatusBadRequest)
		return
	}

	consistenciaDeEscrita, err := extrairConsistenciaDeEscritaDaRequisicao(r, d.Repositorio.ConsistenciaPadraoDeEscrita)
	if err != nil {
		http.Error(w, "consistencia de escrita (w) invalida: "+err.Error(), http.StatusBadRequest)
		return
	}

	leitura := sensors.LeituraDeSensor{
		IdentificadorDoSensor: req.IdentificadorDoSensor,
		DiaDeAgrupamento:      sensors.TruncarParaDiaUTC(instante),
		InstanteDoEvento:      instante.UTC(),
		ValorMedido:           req.ValorMedido,
		UnidadeDeMedida:       req.UnidadeDeMedida,
		EstadoDaLeitura:       req.EstadoDaLeitura,
		AtributosAdicionais:   req.AtributosAdicionais,
	}

	ctx := context.Background()
	inicio := time.Now()
	erro := d.Repositorio.GravarLeituraDeSensor(ctx, leitura, consistenciaDeEscrita)
	duracao := time.Since(inicio)

	res := RespostaDeIngestao{
		Sucesso:     erro == nil,
		DuracaoEmMs: duracao.Milliseconds(),
	}

	if erro != nil {
		res.Erro = erro.Error()
		w.WriteHeader(http.StatusBadGateway)
	}

	log.Printf("ingest_unica_sensor=%s consistencia=%s duracao_ms=%d erro=%v",
		req.IdentificadorDoSensor, consistenciaParaLog(consistenciaDeEscrita), res.DuracaoEmMs, erro != nil)

	w.Header().Set("Content-Type", "Application/json")
	_ = json.NewEncoder(w).Encode(res)
}

func (d DependenciasDoHandler) manipuladorDeRequisicoesDeIngestaoEmLote(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "somente POST", http.StatusMethodNotAllowed)
		return
	}
	r.Body = http.MaxBytesReader(w, r.Body, 5<<20) // 5MB

	var lotes []RequisicaoDeIngestao
	if err := json.NewDecoder(r.Body).Decode(&lotes); err != nil {
		http.Error(w, "json inválido (esperado array): "+err.Error(), http.StatusBadRequest)
		return
	}

	consistenciaDeEscrita, err := extrairConsistenciaDeEscritaDaRequisicao(r, d.Repositorio.ConsistenciaPadraoDeEscrita)
	if err != nil {
		http.Error(w, "consistência w inválida: "+err.Error(), http.StatusBadRequest)
		return
	}

	concorrencia := 8
	if s := r.URL.Query().Get("concorrencia"); s != "" {
		if n, e := strconv.Atoi(s); e == nil && n > 0 && n <= 128 {
			concorrencia = n
		}
	}

	type resultado struct{ ok bool }
	trabalhos := make(chan RequisicaoDeIngestao)
	resultados := make(chan resultado)

	for i := 0; i < concorrencia; i++ {
		go func() {
			for req := range trabalhos {
				instante, err := time.Parse(time.RFC3339, req.InstanteDoEventoISO8601)
				if err != nil {
					resultados <- resultado{ok: false}
					continue
				}
				leitura := sensors.LeituraDeSensor{
					IdentificadorDoSensor: req.IdentificadorDoSensor,
					DiaDeAgrupamento:      sensors.TruncarParaDiaUTC(instante),
					InstanteDoEvento:      instante.UTC(),
					ValorMedido:           req.ValorMedido,
					UnidadeDeMedida:       req.UnidadeDeMedida,
					EstadoDaLeitura:       req.EstadoDaLeitura,
					AtributosAdicionais:   req.AtributosAdicionais,
				}
				err = d.Repositorio.GravarLeituraDeSensor(context.Background(), leitura, consistenciaDeEscrita)
				resultados <- resultado{ok: err == nil}
			}
		}()
	}

	inicio := time.Now()
	go func() {
		for _, req := range lotes {
			trabalhos <- req
		}
		close(trabalhos)
	}()

	aceitos, falhas := 0, 0
	for i := 0; i < len(lotes); i++ {
		if (<-resultados).ok {
			aceitos++
		} else {
			falhas++
		}
	}

	duracao := time.Since(inicio)

	log.Printf("ingest_lote_total=%d aceitos=%d falhas=%d consistencia=%s duracao_ms=%d",
		len(lotes), aceitos, falhas, consistenciaParaLog(consistenciaDeEscrita), duracao.Milliseconds())

	res := RespostaDeIngestao{
		Sucesso:          falhas == 0,
		DuracaoEmMs:      duracao.Milliseconds(),
		QuantidadeAceita: aceitos,
		QuantidadeFalha:  falhas,
	}

	if falhas > 0 {
		w.WriteHeader(http.StatusBadGateway)
	}
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(res)
}

func extrairConsistenciaDeEscritaDaRequisicao(r *http.Request, padrao gocql.Consistency) (gocql.Consistency, error) {
	v := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("w")))
	if v == "" {
		return padrao, nil
	}

	return db.ConverterTextoParaNivelDeConsistencia(v)
}

func consistenciaParaLog(c gocql.Consistency) string {
	switch c {
	case gocql.One:
		return "ONE"
	case gocql.Two:
		return "TWO"
	case gocql.Three:
		return "THREE"
	case gocql.Quorum:
		return "QUORUM"
	case gocql.All:
		return "ALL"
	case gocql.LocalQuorum:
		return "LOCAL_QUORUM"
	case gocql.LocalOne:
		return "LOCAL_ONE"
	default:
		return "UNKNOWN"
	}
}
