package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"sync/atomic"
	"time"
)

type writeReq struct {
	IdentificadorDoSensor   string  `json:"identificador_do_sensor"`
	InstanteDoEventoISO8601 string  `json:"instante_do_evento_iso8601"`
	ValorMedido             float64 `json:"valor_medido"`
}

type leituraResp struct {
	Quantidade int `json:"quantidade"`
}

func main() {
	var (
		baseIngest  = flag.String("ingest", "http://localhost:8080/ingest", "URL /ingest")
		baseRead    = flag.String("read", "http://localhost:8080/leituras/ultima", "URL /leituras/ultima")
		consistW    = flag.String("w", "QUORUM", "Consistência de escrita")
		consistR    = flag.String("r", "ONE", "Consistência de leitura")
		rounds      = flag.Int("rounds", 200, "Rodadas de W->R")
		delayRead   = flag.Duration("delay", 50*time.Millisecond, "Atraso entre W e R")
		sensorID    = flag.String("sensor", "sensor-staleness", "Sensor fixo a testar")
		httpTimeout = flag.Duration("timeout", 5*time.Second, "Timeout HTTP")
	)
	flag.Parse()

	client := &http.Client{Timeout: *httpTimeout}

	var stales int64

	for i := 0; i < *rounds; i++ {
		// write
		wr := writeReq{
			IdentificadorDoSensor:   *sensorID,
			InstanteDoEventoISO8601: time.Now().UTC().Format(time.RFC3339),
			ValorMedido:             rand.Float64()*100 + 1,
		}
		b, _ := json.Marshal(wr)
		ingestURL := fmt.Sprintf("%s?w=%s", *baseIngest, *consistW)
		_, _ = client.Post(ingestURL, "application/json", bytes.NewReader(b))

		time.Sleep(*delayRead)

		// read last
		q := url.Values{}
		q.Set("sensor_id", *sensorID)
		q.Set("r", *consistR)
		readURL := fmt.Sprintf("%s?%s", *baseRead, q.Encode())
		resp, err := client.Get(readURL)
		if err == nil && resp.StatusCode == 200 {
			var lr leituraResp
			_ = json.NewDecoder(resp.Body).Decode(&lr)
			resp.Body.Close()
			if lr.Quantidade == 0 {
				atomic.AddInt64(&stales, 1)
			}
		}
	}

	fmt.Printf("staleness_tester: rounds=%d stale_reads=%d stale_pct=%.2f%%\n", *rounds, stales, 100*float64(stales)/float64(*rounds))
}
