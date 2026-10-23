package main

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"
)

// Only the expected member is extracted, to a fixed destination; archive paths are never used.
func extract(ctx context.Context, archive, destination, member string, max int64) error {
	z, e := zip.OpenReader(archive)
	if e != nil {
		return e
	}
	defer z.Close()
	var selected *zip.File
	for _, f := range z.File {
		if filepath.Base(f.Name) == member && !f.FileInfo().IsDir() {
			if selected != nil {
				return fmt.Errorf("duplicate %s in archive", member)
			}
			selected = f
		}
	}
	if selected == nil {
		return fmt.Errorf("archive missing %s", member)
	}
	if selected.UncompressedSize64 > uint64(max) {
		return fmt.Errorf("CSV exceeds extraction limit")
	}
	r, e := selected.Open()
	if e != nil {
		return e
	}
	defer r.Close()
	w, e := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer w.Close()
	n, e := io.Copy(w, io.LimitReader(contextReader{ctx, r}, max+1))
	if e != nil {
		return e
	}
	if n > max {
		return fmt.Errorf("CSV exceeds extraction limit")
	}
	return w.Close()
}

type contextReader struct {
	ctx context.Context
	r   io.Reader
}

func (r contextReader) Read(p []byte) (int, error) {
	if e := r.ctx.Err(); e != nil {
		return 0, e
	}
	return r.r.Read(p)
}
func download(ctx context.Context, url, destination string, max int64) error {
	req, e := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if e != nil {
		return e
	}
	req.Header.Set("User-Agent", "entreprises-registry/1.0")
	// Go's client follows redirects (up to ten) and streams the response to disk.
	client := &http.Client{Timeout: 6 * time.Hour}
	res, e := client.Do(req)
	if e != nil {
		return e
	}
	defer res.Body.Close()
	if res.StatusCode != http.StatusOK {
		return fmt.Errorf("download HTTP %d", res.StatusCode)
	}
	if res.ContentLength > max {
		return fmt.Errorf("archive exceeds download limit")
	}
	f, e := os.OpenFile(destination, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if e != nil {
		return e
	}
	defer f.Close()
	n, e := io.Copy(f, io.LimitReader(res.Body, max+1))
	if e != nil {
		return e
	}
	if n > max {
		return fmt.Errorf("archive exceeds download limit")
	}
	return f.Close()
}
func (s *store) refresh(ctx context.Context, localEst, localLegal string) (err error) {
	s.mu.Lock()
	if s.refreshing {
		s.mu.Unlock()
		return fmt.Errorf("refresh already running")
	}
	s.refreshing = true
	s.mu.Unlock()
	start := time.Now()
	slog.Info("registry refresh started")
	defer func() {
		s.mu.Lock()
		s.refreshing = false
		if err != nil {
			s.lastError = err.Error()
		} else {
			s.lastError = ""
		}
		s.mu.Unlock()
		if err != nil {
			slog.Error("registry refresh failed", "error", err)
		} else {
			slog.Info("registry refresh completed", "duration", time.Since(start))
		}
	}()
	ctx, cancel := context.WithTimeout(ctx, s.cfg.RefreshTimeout)
	defer cancel()
	work, err := os.MkdirTemp(s.cfg.DataDir, "refresh-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(work)
	if localEst == "" {
		for _, src := range []struct {
			url, member string
			dest        *string
		}{{s.cfg.EstablishmentURL, "StockEtablissement_utf8.csv", &localEst}, {s.cfg.LegalUnitURL, "StockUniteLegale_utf8.csv", &localLegal}} {
			archive := filepath.Join(work, "source.zip")
			slog.Info("downloading registry archive", "member", src.member)
			if err = download(ctx, src.url, archive, s.cfg.MaxDownloadBytes); err != nil {
				return err
			}
			*src.dest = filepath.Join(work, src.member)
			if err = extract(ctx, archive, *src.dest, src.member, s.cfg.MaxExtractBytes); err != nil {
				return err
			}
			if err = os.Remove(archive); err != nil {
				return err
			}
		}
	}
	version := fmt.Sprintf("snapshot-%d.duckdb", time.Now().UnixNano())
	path := filepath.Join(s.cfg.DataDir, version)
	defer func() {
		if err != nil {
			os.Remove(path)
			os.Remove(path + ".wal")
		}
		os.RemoveAll(path + ".tmp")
	}()
	slog.Info("importing registry into DuckDB")
	count, err := buildSnapshot(ctx, s.cfg, localEst, localLegal, path)
	if err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	return s.publish(path, version, count)
}
func (s *store) schedule(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		s.mu.RLock()
		due := time.Until(s.refreshed.Add(s.cfg.RefreshInterval))
		s.mu.RUnlock()
		if due <= 0 {
			if e := s.refresh(ctx, "", ""); e != nil {
				due = s.cfg.RetryInterval
			} else {
				due = s.cfg.RefreshInterval
			}
		}
		timer.Reset(due)
	}
}
