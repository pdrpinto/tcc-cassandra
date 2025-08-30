package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

type leitura struct {
	Quantidade int `json:"quantidade"`
}

func main() {
	var (
		baseURL     = flag.String("url", "http://localhost:8080/leituras/ultimas", "URL do endpoint de leitura")
		consistR    = flag.String("r", "QUORUM", "Consistência de leitura (?r=")
		dur         = flag.Duration("duracao", 10*time.Second, "Duração do teste")
		rps         = flag.Int("rps", 200, "Taxa de requisições por segundo")
		conc        = flag.Int("conc", runtime.NumCPU(), "Concorrência")
		sensores    = flag.Int("sensores", 100, "Quantidade de sensores distintos")
		limite      = flag.Int("limite", 10, "Limite por consulta")
		timeoutHTTP = flag.Duration("timeout", 5*time.Second, "Timeout HTTP")
		outCSV      = flag.String("out", "", "Arquivo CSV de saída (opcional)")
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
				sensorID := fmt.Sprintf("sensor-%d", rand.Intn(*sensores))
				q := url.Values{}
				q.Set("sensor_id", sensorID)
				q.Set("limite", fmt.Sprintf("%d", *limite))
				q.Set("r", *consistR)
				fullURL := fmt.Sprintf("%s?%s", *baseURL, q.Encode())
				t0 := time.Now()
				resp, err := client.Get(fullURL)
				if err != nil {
					latCh <- -1
					return
				}
				defer resp.Body.Close()
				if resp.StatusCode == 200 {
					var lr leitura
					_ = json.NewDecoder(resp.Body).Decode(&lr)
					_ = lr
					latCh <- time.Since(t0)
				} else {
					latCh <- -1
				}
			}()
		}
	}
FIM:
	wg.Wait()
	close(latCh)

	durReal := time.Since(start)
	fmt.Printf("leitura_bench fim: total=%d ok=%d duracao_ms=%d\n", total, okCount, durReal.Milliseconds())

	if *outCSV != "" {
		f, err := os.Create(*outCSV)
		if err == nil {
			defer f.Close()
			_, _ = f.WriteString("metric,valor\n")
			_, _ = f.WriteString(fmt.Sprintf("total,%d\n", total))
			_, _ = f.WriteString(fmt.Sprintf("ok,%d\n", okCount))
			_, _ = f.WriteString(fmt.Sprintf("duracao_ms,%d\n", durReal.Milliseconds()))
		}
	}
}
