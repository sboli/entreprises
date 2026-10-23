package main

import (
	"context"
	"database/sql"
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	_ "github.com/duckdb/duckdb-go/v2"
)

type store struct {
	mu         sync.RWMutex
	db         *sql.DB
	version    string
	refreshed  time.Time
	count      int64
	refreshing bool
	lastError  string
	cfg        config
}

func literal(s string) string { return "'" + strings.ReplaceAll(s, "'", "''") + "'" }
func openDB(path string, c config, readOnly bool) (*sql.DB, error) {
	dsn := path
	if readOnly {
		dsn += "?access_mode=read_only"
	}
	db, e := sql.Open("duckdb", dsn)
	if e != nil {
		return nil, e
	}
	db.SetMaxOpenConns(1)
	for _, q := range []string{"SET memory_limit=" + literal(c.MemoryLimit), fmt.Sprintf("SET threads=%d", c.Threads), "SET enable_external_access=false"} {
		// External access is disabled only for the API connection; the importer reads local CSVs.
		if !readOnly && strings.Contains(q, "external_access") {
			continue
		}
		if _, e = db.Exec(q); e != nil {
			db.Close()
			return nil, e
		}
	}
	return db, nil
}
func (s *store) load() error {
	b, e := os.ReadFile(filepath.Join(s.cfg.DataDir, "CURRENT"))
	if errors.Is(e, os.ErrNotExist) {
		return nil
	}
	if e != nil {
		return e
	}
	version := strings.TrimSpace(string(b))
	if filepath.Base(version) != version || !strings.HasPrefix(version, "snapshot-") || !strings.HasSuffix(version, ".duckdb") {
		return errors.New("invalid CURRENT manifest")
	}
	db, e := openDB(filepath.Join(s.cfg.DataDir, version), s.cfg, true)
	if e != nil {
		return e
	}
	var refreshed time.Time
	var count int64
	e = db.QueryRow("SELECT refreshed_at, row_count FROM metadata").Scan(&refreshed, &count)
	if e != nil {
		db.Close()
		return e
	}
	s.db = db
	s.version = version
	s.refreshed = refreshed
	s.count = count
	return nil
}
func (s *store) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.db != nil {
		s.db.Close()
	}
}

func csvView(ctx context.Context, db *sql.DB, name, path string, required []string) error {
	f, e := os.Open(path)
	if e != nil {
		return e
	}
	defer f.Close()
	header, e := csv.NewReader(f).Read()
	if e != nil {
		return e
	}
	found := map[string]bool{}
	for _, h := range header {
		found[h] = true
	}
	for _, h := range required {
		if !found[h] {
			return fmt.Errorf("%s missing column %s", name, h)
		}
	}
	_, e = db.ExecContext(ctx, "CREATE VIEW "+name+" AS SELECT * FROM read_csv("+literal(path)+", header=true, all_varchar=true, delim=',', quote='\"', escape='\"', nullstr=['','[ND]'], strict_mode=true)")
	return e
}
func buildSnapshot(ctx context.Context, c config, establishments, legal, path string) (int64, error) {
	db, e := openDB(path, c, false)
	if e != nil {
		return 0, e
	}
	defer db.Close()
	if _, e = db.ExecContext(ctx, "SET temp_directory="+literal(path+".tmp")); e != nil {
		return 0, e
	}
	if e = csvView(ctx, db, "establishment_csv", establishments, []string{"siren", "siret", "statutDiffusionEtablissement", "etablissementSiege", "etatAdministratifEtablissement", "denominationUsuelleEtablissement", "enseigne1Etablissement", "enseigne2Etablissement", "enseigne3Etablissement", "numeroVoieEtablissement", "indiceRepetitionEtablissement", "typeVoieEtablissement", "libelleVoieEtablissement", "codePostalEtablissement", "libelleCommuneEtablissement", "activitePrincipaleEtablissement"}); e != nil {
		return 0, e
	}
	if e = csvView(ctx, db, "legal_csv", legal, []string{"siren", "statutDiffusionUniteLegale", "etatAdministratifUniteLegale", "denominationUniteLegale", "nomUniteLegale", "prenom1UniteLegale", "denominationUsuelle1UniteLegale", "denominationUsuelle2UniteLegale", "denominationUsuelle3UniteLegale", "sigleUniteLegale", "categorieJuridiqueUniteLegale"}); e != nil {
		return 0, e
	}
	queries := []string{
		`CREATE TEMP TABLE legal AS SELECT siren,
 coalesce(denominationUniteLegale, nullif(trim(concat_ws(' ',prenom1UniteLegale,nomUniteLegale)),''),'') AS legal_name,
 concat_ws(' ',denominationUsuelle1UniteLegale,denominationUsuelle2UniteLegale,denominationUsuelle3UniteLegale,sigleUniteLegale) AS aliases,
 coalesce(categorieJuridiqueUniteLegale,'') AS legal_form, etatAdministratifUniteLegale='A' AS legal_active
 FROM legal_csv WHERE statutDiffusionUniteLegale='O' AND regexp_full_match(siren,'[0-9]{9}')`,
		`CREATE TABLE companies AS SELECT e.siret,e.siren,l.legal_name,
 coalesce(e.denominationUsuelleEtablissement,e.enseigne1Etablissement,e.enseigne2Etablissement,e.enseigne3Etablissement,l.legal_name,'') AS name,
 coalesce(e.denominationUsuelleEtablissement,e.enseigne1Etablissement,e.enseigne2Etablissement,e.enseigne3Etablissement,'') AS trade_name,
 trim(concat_ws(' ',e.numeroVoieEtablissement,e.indiceRepetitionEtablissement,e.typeVoieEtablissement,e.libelleVoieEtablissement)) AS address,
 coalesce(e.codePostalEtablissement,'') AS postal_code,coalesce(e.libelleCommuneEtablissement,'') AS city,
 l.legal_form,coalesce(e.activitePrincipaleEtablissement,'') AS activity,
 coalesce(e.etablissementSiege='true',false) AS headquarters,
 coalesce(e.etatAdministratifEtablissement='A' AND l.legal_active,false) AS active,
 lower(strip_accents(concat_ws(' ',l.legal_name,l.aliases,e.denominationUsuelleEtablissement,e.enseigne1Etablissement,e.enseigne2Etablissement,e.enseigne3Etablissement))) AS search_name
 FROM establishment_csv e JOIN legal l ON e.siren=l.siren
 WHERE e.statutDiffusionEtablissement='O' AND regexp_full_match(e.siret,'[0-9]{14}') ORDER BY e.siret`,
		`CREATE UNIQUE INDEX companies_siret ON companies(siret)`,
		`CREATE INDEX companies_siren ON companies(siren)`,
		`DROP VIEW establishment_csv`, `DROP VIEW legal_csv`,
		`CREATE TABLE metadata AS SELECT current_timestamp AS refreshed_at,count(*) AS row_count FROM companies`,
		`CHECKPOINT`,
	}
	for _, q := range queries {
		if _, e = db.ExecContext(ctx, q); e != nil {
			return 0, e
		}
	}
	var count int64
	e = db.QueryRowContext(ctx, "SELECT row_count FROM metadata").Scan(&count)
	if e == nil && count == 0 {
		e = errors.New("refusing to publish empty registry")
	}
	return count, e
}
func (s *store) publish(path, version string, count int64) error {
	db, e := openDB(path, s.cfg, true)
	if e != nil {
		return e
	}
	var refreshed time.Time
	if e = db.QueryRow("SELECT refreshed_at FROM metadata").Scan(&refreshed); e != nil {
		db.Close()
		return e
	}
	// Flush the database before atomically publishing its manifest.
	f, e := os.Open(path)
	if e == nil {
		e = f.Sync()
		f.Close()
	}
	if e != nil {
		db.Close()
		return e
	}
	manifest := filepath.Join(s.cfg.DataDir, "CURRENT.new")
	f, e = os.OpenFile(manifest, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0600)
	if e != nil {
		db.Close()
		return e
	}
	_, e = f.WriteString(version + "\n")
	if e == nil {
		e = f.Sync()
	}
	ce := f.Close()
	if e == nil {
		e = ce
	}
	if e == nil {
		e = os.Rename(manifest, filepath.Join(s.cfg.DataDir, "CURRENT"))
	}
	if e != nil {
		db.Close()
		return e
	}
	s.mu.Lock()
	old, oldVersion := s.db, s.version
	s.db = db
	s.version = version
	s.refreshed = refreshed
	s.count = count
	if old != nil {
		old.Close()
	}
	s.mu.Unlock()
	if dir, e := os.Open(s.cfg.DataDir); e == nil {
		_ = dir.Sync()
		dir.Close()
	}
	if oldVersion != "" {
		_ = os.Remove(filepath.Join(s.cfg.DataDir, oldVersion))
	}
	return nil
}
