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

type RespostaDeLeituras struct {
	Quantidade int                       `json:"quantidade"`
	Itens      []sensors.LeituraDeSensor `json:"itens"`
	DuracaoMs  int64                     `json:"duracao_ms"`
}

func RegistrarRotasDeLeitura(mux *http.ServeMux, dep DependenciasDoHandler) {
	mux.HandleFunc("/leituras/ultimas", func(w http.ResponseWriter, r *http.Request) {

		manipuladorDeConsultaUltimas(w, r, dep.Repositorio)
	})

	// Intervalo de tempo (data + inicio/fim). Certifique-se de ter a função manipuladorDeConsultaIntervalo.
	mux.HandleFunc("/leituras/intervalo", func(w http.ResponseWriter, r *http.Request) {
		manipuladorDeConsultaIntervalo(w, r, dep.Repositorio)
	})

	// Última leitura (LIMIT 1 no bucket de hoje; se vazio, tenta ontem).
	// Implemente manipuladorDeConsultaUltima ou remova esta rota por enquanto.
	mux.HandleFunc("/leituras/ultima", func(w http.ResponseWriter, r *http.Request) {
		manipuladorDeConsultaUltima(w, r, dep.Repositorio)
	})
}

func manipuladorDeConsultaUltimas(w http.ResponseWriter, r *http.Request, repo *sensors.RepositorioDeLeiturasDeSensores) {
	if r.Method != http.MethodGet {
		http.Error(w, "somente GET", http.StatusMethodNotAllowed)
		return
	}
	sensorID := strings.TrimSpace(r.URL.Query().Get("sensor_id"))
	if sensorID == "" {
		http.Error(w, "sensor_id é obrigatório", http.StatusBadRequest)
		return
	}

	// Data bucket padrao: dia de hoje (UTC)

	dia := time.Now().UTC()
	if s := strings.TrimSpace(r.URL.Query().Get("data")); s != "" {
		parsed, err := time.Parse("2006-01-02", s)
		if err != nil {
			http.Error(w, "parametro de data invalido (use yyyy-mm--dd utc)", http.StatusBadRequest)
			return
		}
		dia = parsed.UTC()
	}
	dia = sensors.TruncarParaDiaUTC(dia)

	limite := 50
	if s := r.URL.Query().Get("limite"); s != "" {
		if n, err := strconv.Atoi(s); err == nil && n > 0 {
			if n > 5000 {
				n = 5000
			}
			limite = n
		}
	}

	consistencia, err := extrairConsistenciaDeLeituraDaRequisicao(r, repo.ConsistenciaPadraoDeLeitura)
	if err != nil {
		http.Error(w, "consistencia r invalida: "+err.Error(), http.StatusBadRequest)
		return
	}

	ctx := context.Background()
	inicio := time.Now()
	leituras, erro := repo.ConsultarUltimasLeiturasPorSensor(ctx, sensorID, dia, limite, consistencia)
	duracao := time.Since(inicio)

	if erro != nil {
		http.Error(w, "falha na consulta: "+erro.Error(), http.StatusBadGateway)
		log.Printf("leituras/ultimas sensor=%s r=%s erro=%v", sensorID, consistenciaParaLog(consistencia), erro)
		return
	}

	resp := RespostaDeLeituras{
		Quantidade: len(leituras),
		Itens:      leituras,
		DuracaoMs:  duracao.Milliseconds(),
	}
	log.Printf("leituras/ultimas sensor=%s r=%s qtd=%d ms=%d", sensorID, consistenciaParaLog(consistencia), len(leituras), resp.DuracaoMs)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func manipuladorDeConsultaIntervalo(w http.ResponseWriter, r *http.Request, repo *sensors.RepositorioDeLeiturasDeSensores) {
	if r.Method != http.MethodGet {
		http.Error(w, "somente GET", http.StatusMethodNotAllowed)
		return
	}
	q := r.URL.Query()
	sensorID := strings.TrimSpace(q.Get("sensor_id"))
	if sensorID == "" {
		http.Error(w, "parametro obrigatorio: sensor_id", http.StatusBadRequest)
		return
	}

	dataStr := strings.TrimSpace(q.Get("data"))
	if dataStr == "" {
		http.Error(w, "parametro obrigatorio: data (YYYY-MM-DD UTC)", http.StatusBadRequest)
		return
	}
	dia, err := time.Parse("2006-01-02", dataStr)
	if err != nil {
		http.Error(w, "parametro data invalido (use YYYY-MM-DD UTC)", http.StatusBadRequest)
		return
	}
	dia = sensors.TruncarParaDiaUTC(dia.UTC())

	inicioStr := strings.TrimSpace(q.Get("inicio"))
	fimStr := strings.TrimSpace(q.Get("fim"))
	if inicioStr == "" || fimStr == "" {
		http.Error(w, "parametros obrigatorios: inicio,fim (RFC3339 UTC)", http.StatusBadRequest)
		return
	}
	inicioTs, err := time.Parse(time.RFC3339, inicioStr)
	if err != nil {
		http.Error(w, "inicio invalido (use RFC3339 UTC)", http.StatusBadRequest)
		return
	}
	fimTs, err := time.Parse(time.RFC3339, fimStr)
	if err != nil {
		http.Error(w, "fim invalido (use RFC3339 UTC)", http.StatusBadRequest)
		return
	}
	if !fimTs.After(inicioTs) && !fimTs.Equal(inicioTs) {
		http.Error(w, "fim deve ser >= inicio", http.StatusBadRequest)
		return
	}

	limite := 1000
	if s := q.Get("limite"); s != "" {
		if n, e := strconv.Atoi(s); e == nil && n > 0 {
			if n > 10000 {
				n = 10000
			}
			limite = n
		}
	}

	consistencia, err := extrairConsistenciaDeLeituraDaRequisicao(r, repo.ConsistenciaPadraoDeLeitura)
	if err != nil {
		http.Error(w, "consistencia r invalida: "+err.Error(), http.StatusBadRequest)
		return
	}

	ctx := context.Background()
	inicio := time.Now()
	leituras, erro := repo.ConsultarLeiturasPorIntervaloDeTempo(ctx, sensorID, dia, inicioTs.UTC(), fimTs.UTC(), limite, consistencia)
	duracao := time.Since(inicio)

	if erro != nil {
		http.Error(w, "falha na consulta: "+erro.Error(), http.StatusBadGateway)
		log.Printf("leituras/intervalo sensor=%s r=%s erro=%v", sensorID, consistenciaParaLog(consistencia), erro)
		return
	}

	resp := RespostaDeLeituras{
		Quantidade: len(leituras),
		Itens:      leituras,
		DuracaoMs:  duracao.Milliseconds(),
	}
	log.Printf("leituras/intervalo sensor=%s r=%s qtd=%d ms=%d", sensorID, consistenciaParaLog(consistencia), len(leituras), resp.DuracaoMs)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)
}

func manipuladorDeConsultaUltima(w http.ResponseWriter, r *http.Request, repo *sensors.RepositorioDeLeiturasDeSensores) {
	if r.Method != http.MethodGet {
		http.Error(w, "somente GET", http.StatusMethodNotAllowed)
		return
	}
	sensorID := strings.TrimSpace(r.URL.Query().Get("sensor_id"))
	if sensorID == "" {
		http.Error(w, "parametro obrigatorio: sensor_id", http.StatusBadRequest)
		return
	}

	consistencia, err := extrairConsistenciaDeLeituraDaRequisicao(r, repo.ConsistenciaPadraoDeLeitura)
	if err != nil {
		http.Error(w, "consistencia r invalida: "+err.Error(), http.StatusBadRequest)
		return
	}

	// Estratégia sem tabela auxiliar: tenta hoje; se vazio, tenta ontem.
	hoje := sensors.TruncarParaDiaUTC(time.Now().UTC())
	ontem := hoje.Add(-24 * time.Hour)

	ctx := context.Background()
	inicio := time.Now()

	// Tenta hoje
	leituras, erro := repo.ConsultarUltimasLeiturasPorSensor(ctx, sensorID, hoje, 1, consistencia)
	if erro == nil && len(leituras) == 0 {
		// Tenta ontem
		leituras, erro = repo.ConsultarUltimasLeiturasPorSensor(ctx, sensorID, ontem, 1, consistencia)
	}
	duracao := time.Since(inicio)

	if erro != nil {
		http.Error(w, "falha na consulta: "+erro.Error(), http.StatusBadGateway)
		log.Printf("leituras/ultima sensor=%s r=%s erro=%v", sensorID, consistenciaParaLog(consistencia), erro)
		return
	}
	resp := RespostaDeLeituras{
		Quantidade: len(leituras),
		Itens:      leituras,
		DuracaoMs:  duracao.Milliseconds(),
	}
	log.Printf("leituras/ultima sensor=%s r=%s qtd=%d ms=%d", sensorID, consistenciaParaLog(consistencia), len(leituras), resp.DuracaoMs)

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(resp)

}

func extrairConsistenciaDeLeituraDaRequisicao(r *http.Request, padrao gocql.Consistency) (gocql.Consistency, error) {
	v := strings.ToUpper(strings.TrimSpace(r.URL.Query().Get("r")))
	if v == "" {
		return padrao, nil
	}

	return db.ConverterTextoParaNivelDeConsistencia(v)
}
