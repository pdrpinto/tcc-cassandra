package main

import (
	"context"
	"flag"
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
)

func main() {
	var (
		hosts    = flag.String("hosts", "127.0.0.1:9042", "Lista de hosts cassandra separados por vírgula")
		keyspace = flag.String("keyspace", "tcc", "Keyspace")
		wlevel   = flag.String("w", "QUORUM", "Consistência de escrita")
		dur      = flag.Duration("duracao", 10*time.Second, "Duração")
		rps      = flag.Int("rps", 500, "Taxa de inserts por segundo")
		conc     = flag.Int("conc", runtime.NumCPU(), "Concorrência")
	)
	flag.Parse()

	cluster := gocql.NewCluster(split(*hosts)...)
	cluster.Keyspace = *keyspace
	cluster.ProtoVersion = 4
	cluster.Timeout = 5 * time.Second
	cluster.Consistency = mustParse(*wlevel)

	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}
	defer session.Close()

	ctx, cancel := context.WithTimeout(context.Background(), *dur)
	defer cancel()

	var total, okCount int64
	wg := &sync.WaitGroup{}
	tick := time.NewTicker(time.Second / time.Duration(*rps))
	defer tick.Stop()

	sem := make(chan struct{}, *conc)
	start := time.Now()

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
				t := time.Now().UTC()
				q := session.Query(`INSERT INTO sensor_readings (sensor_id, day_bucket, ts, value, unit, status, tags) VALUES (?, ?, ?, ?, ?, ?, ?)`,
					"bench-db", time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC), gocql.UUIDFromTime(t), 1.23, "C", int16(0), map[string]string{"src": "db-bench"},
				).Consistency(cluster.Consistency).WithContext(context.Background())
				if err := q.Exec(); err == nil {
					atomic.AddInt64(&okCount, 1)
				}
				atomic.AddInt64(&total, 1)
			}()
		}
	}
FIM:
	wg.Wait()
	durReal := time.Since(start)
	fmt.Printf("db_bench: total=%d ok=%d duracao_ms=%d\n", total, okCount, durReal.Milliseconds())
}

func split(s string) []string {
	var out []string
	cur := ""
	for _, ch := range s {
		if ch == ',' {
			out = append(out, cur)
			cur = ""
		} else {
			cur += string(ch)
		}
	}
	if cur != "" {
		out = append(out, cur)
	}
	return out
}

func mustParse(c string) gocql.Consistency {
	switch c {
	case "ONE":
		return gocql.One
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
