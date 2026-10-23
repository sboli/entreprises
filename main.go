package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

func run() error {
	c, e := loadConfig()
	if e != nil {
		return e
	}
	if len(os.Args) > 1 && os.Args[1] == "healthcheck" {
		client := http.Client{Timeout: 3 * time.Second}
		_, port, e := net.SplitHostPort(c.Address)
		if e != nil {
			return e
		}
		r, e := client.Get("http://127.0.0.1:" + port + "/healthz")
		if e != nil {
			return e
		}
		r.Body.Close()
		if r.StatusCode != 200 {
			return fmt.Errorf("unhealthy")
		}
		return nil
	}
	if e = os.MkdirAll(c.DataDir, 0700); e != nil {
		return e
	}
	// A DuckDB volume has exactly one owning process, including offline imports.
	lock, e := os.OpenFile(filepath.Join(c.DataDir, "LOCK"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		return e
	}
	defer lock.Close()
	if e = unix.Flock(int(lock.Fd()), unix.LOCK_EX|unix.LOCK_NB); e != nil {
		return fmt.Errorf("data directory already in use: %w", e)
	}
	s := &store{cfg: c}
	if e = s.load(); e != nil {
		return e
	}
	defer s.close()
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	if len(os.Args) > 1 {
		if os.Args[1] != "import" {
			return fmt.Errorf("usage: entreprises [import --establishments file.csv --legal-units file.csv | healthcheck]")
		}
		f := flag.NewFlagSet("import", flag.ContinueOnError)
		est := f.String("establishments", "", "establishments CSV")
		legal := f.String("legal-units", "", "legal units CSV")
		if e = f.Parse(os.Args[2:]); e != nil {
			return e
		}
		if *est == "" || *legal == "" {
			return fmt.Errorf("both CSV paths required")
		}
		return s.refresh(ctx, *est, *legal)
	}
	// These are uncommitted refresh artifacts left by an interrupted process.
	for _, pattern := range []string{"refresh-*", "snapshot-*.duckdb.tmp"} {
		matches, _ := filepath.Glob(filepath.Join(c.DataDir, pattern))
		for _, p := range matches {
			if e = os.RemoveAll(p); e != nil {
				return e
			}
		}
	}
	// Remove orphan snapshots only after successfully loading the published one.
	matches, _ := filepath.Glob(filepath.Join(c.DataDir, "snapshot-*.duckdb*"))
	for _, p := range matches {
		if filepath.Base(p) != s.version {
			if e = os.RemoveAll(p); e != nil {
				return e
			}
		}
	}
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); s.schedule(ctx) }()
	defer func() { cancel(); wg.Wait() }()
	server := &http.Server{Addr: c.Address, Handler: s.handler(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: c.QueryTimeout + 5*time.Second, IdleTimeout: 60 * time.Second, MaxHeaderBytes: 16 << 10}
	done := make(chan error, 1)
	go func() { slog.Info("registry API listening", "address", c.Address); done <- server.ListenAndServe() }()
	select {
	case e = <-done:
		cancel()
	case <-ctx.Done():
	}
	shutdown, cancelShutdown := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancelShutdown()
	if se := server.Shutdown(shutdown); se != nil {
		server.Close()
		return se
	}
	if e != nil && e != http.ErrServerClosed {
		return e
	}
	return nil
}
func main() {
	if e := run(); e != nil {
		slog.Error("service stopped", "error", e)
		os.Exit(1)
	}
}
