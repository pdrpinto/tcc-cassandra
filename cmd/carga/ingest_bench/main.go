package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type req struct {
	IdentificadorDoSensor   string            `json:"identificador_do_sensor"`
	InstanteDoEventoISO8601 string            `json:"instante_do_evento_iso8601"`
	ValorMedido             float64           `json:"valor_medido"`
	UnidadeDeMedida         string            `json:"unidade_de_medida,omitempty"`
	EstadoDaLeitura         int16             `json:"estado_da_leitura,omitempty"`
	AtributosAdicionais     map[string]string `json:"atributos_adicionais,omitempty"`
}

type result struct {
	ok      bool
	latency time.Duration
}

func main() {
	var (
		baseURL     = flag.String("url", "http://localhost:8080/ingest", "URL do endpoint /ingest")
		consistW    = flag.String("w", "QUORUM", "Consistência de escrita (?w=)")
		dur         = flag.Duration("duracao", 10*time.Second, "Duração do teste")
		rps         = flag.Int("rps", 200, "Taxa de requisições por segundo")
		conc        = flag.Int("conc", runtime.NumCPU(), "Concorrência")
		sensores    = flag.Int("sensores", 100, "Quantidade de sensores distintos")
		outCSV      = flag.String("out", "", "Arquivo CSV de saída (opcional)")
		timeoutHTTP = flag.Duration("timeout", 5*time.Second, "Timeout HTTP")
	)
	flag.Parse()

	client := &http.Client{Timeout: *timeoutHTTP}

	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()

	var total, okCount int64
	latCh := make(chan time.Duration, 10000)

	// Geradores de carga
	wg := &sync.WaitGroup{}
	tick := time.NewTicker(time.Second / time.Duration(*rps))
	defer tick.Stop()

	sem := make(chan struct{}, *conc)
	start := time.Now()

	go func() {
		for l := range latCh {
			atomic.AddInt64(&total, 1)
			if l >= 0 {
				atomic.AddInt64(&okCount, 1)
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			goto FIM
		case <-tick.C:
			sem <- struct{}{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer func() { <-sem }()
				// payload
				sensorID := fmt.Sprintf("sensor-%d", rand.Intn(*sensores))
				payload := req{
					IdentificadorDoSensor:   sensorID,
					InstanteDoEventoISO8601: time.Now().UTC().Format(time.RFC3339),
					ValorMedido:             rand.Float64()*100 + 1,
					UnidadeDeMedida:         "C",
				}
				b, _ := json.Marshal(payload)
				url := fmt.Sprintf("%s?w=%s", *baseURL, *consistW)
				t0 := time.Now()
				resp, err := client.Post(url, "application/json", bytes.NewReader(b))
				if err != nil {
					latCh <- -1
					return
				}
				_ = resp.Body.Close()
				latCh <- time.Since(t0)
			}()
		}
	}
FIM:
	wg.Wait()
	close(latCh)

	durReal := time.Since(start)
	fmt.Printf("ingest_bench fim: total=%d ok=%d duracao_ms=%d\n", total, okCount, durReal.Milliseconds())

	if *outCSV != "" {
		f, err := os.Create(*outCSV)
		if err == nil {
			defer f.Close()
			// cabeçalho
			_, _ = f.WriteString("metric,valor\n")
			_, _ = f.WriteString(fmt.Sprintf("total,%d\n", total))
			_, _ = f.WriteString(fmt.Sprintf("ok,%d\n", okCount))
			_, _ = f.WriteString(fmt.Sprintf("duracao_ms,%d\n", durReal.Milliseconds()))
		}
	}
}
