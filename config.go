package main

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

const establishmentURL = "https://www.data.gouv.fr/api/1/datasets/r/0651fb76-bcf3-4f6a-a38d-bc04fa708576"
const legalUnitURL = "https://www.data.gouv.fr/api/1/datasets/r/825f4199-cadd-486c-ac46-a65a8ea1a047"

type config struct {
	DataDir, Address, EstablishmentURL, LegalUnitURL, MemoryLimit, ImportMemoryLimit, APIKey string
	RefreshInterval, RetryInterval, RefreshTimeout, QueryTimeout                             time.Duration
	Threads, ImportThreads, QueryConcurrency                                                 int
	MaxDownloadBytes, MaxExtractBytes                                                        int64
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
func loadConfig() (config, error) {
	c := config{DataDir: env("DATA_DIR", "/data"), Address: env("LISTEN_ADDR", ":8080"), EstablishmentURL: env("ESTABLISHMENTS_URL", establishmentURL), LegalUnitURL: env("LEGAL_UNITS_URL", legalUnitURL), MemoryLimit: env("DUCKDB_MEMORY_LIMIT", "2GB"), APIKey: os.Getenv("API_KEY")}
	c.ImportMemoryLimit = env("DUCKDB_IMPORT_MEMORY_LIMIT", c.MemoryLimit)
	for _, x := range []struct {
		k, d string
		p    *time.Duration
	}{
		{"REFRESH_INTERVAL", "168h", &c.RefreshInterval}, {"RETRY_INTERVAL", "1h", &c.RetryInterval}, {"REFRESH_TIMEOUT", "12h", &c.RefreshTimeout}, {"QUERY_TIMEOUT", "15s", &c.QueryTimeout},
	} {
		v, e := time.ParseDuration(env(x.k, x.d))
		if e != nil || v <= 0 {
			return c, fmt.Errorf("invalid %s", x.k)
		}
		*x.p = v
	}
	for _, x := range []struct {
		k, d string
		p    *int
	}{{"DUCKDB_THREADS", "2", &c.Threads}, {"DUCKDB_IMPORT_THREADS", "1", &c.ImportThreads}, {"QUERY_CONCURRENCY", "4", &c.QueryConcurrency}} {
		v, e := strconv.Atoi(env(x.k, x.d))
		if e != nil || v < 1 || v > 64 {
			return c, fmt.Errorf("invalid %s", x.k)
		}
		*x.p = v
	}
	for _, x := range []struct {
		k, d string
		p    *int64
	}{{"MAX_DOWNLOAD_BYTES", "10737418240", &c.MaxDownloadBytes}, {"MAX_EXTRACT_BYTES", "42949672960", &c.MaxExtractBytes}} {
		v, e := strconv.ParseInt(env(x.k, x.d), 10, 64)
		if e != nil || v < 1 || v >= 1<<62 {
			return c, fmt.Errorf("invalid %s", x.k)
		}
		*x.p = v
	}
	return c, nil
}
