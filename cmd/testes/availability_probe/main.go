package main

import (
	"flag"
	"fmt"
	"net/http"
	"time"
)

func main() {
	var (
		url      = flag.String("url", "http://localhost:8080/healthz", "URL de health/read a testar")
		interval = flag.Duration("interval", 500*time.Millisecond, "Intervalo entre probes")
		duracao  = flag.Duration("duracao", 30*time.Second, "Duração total")
		timeout  = flag.Duration("timeout", 2*time.Second, "Timeout HTTP")
	)
	flag.Parse()

	client := &http.Client{Timeout: *timeout}
	deadline := time.Now().Add(*duracao)

	var total, sucesso int

	for time.Now().Before(deadline) {
		total++
		resp, err := client.Get(*url)
		if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 500 { // 5xx conta como indisponível
			sucesso++
		}
		if resp != nil {
			resp.Body.Close()
		}
		time.Sleep(*interval)
	}

	disponibilidade := 0.0
	if total > 0 {
		disponibilidade = float64(sucesso) / float64(total) * 100
	}
	fmt.Printf("availability_probe: total=%d sucesso=%d disponibilidade=%.2f%%\n", total, sucesso, disponibilidade)
}
