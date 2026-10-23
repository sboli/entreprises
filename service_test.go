package main

import (
	"archive/zip"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func testConfig(t *testing.T) config {
	t.Helper()
	return config{DataDir: t.TempDir(), MemoryLimit: "256MB", Threads: 1, QueryConcurrency: 2, QueryTimeout: time.Second * 10, RefreshTimeout: time.Minute, RefreshInterval: 168 * time.Hour, RetryInterval: time.Hour, MaxDownloadBytes: 1 << 20, MaxExtractBytes: 1 << 20}
}
func fixture(t *testing.T, dir, name string, header []string, rows []map[string]string) string {
	t.Helper()
	p := filepath.Join(dir, name)
	f, e := os.Create(p)
	if e != nil {
		t.Fatal(e)
	}
	w := csv.NewWriter(f)
	_ = w.Write(header)
	for _, row := range rows {
		v := make([]string, len(header))
		for i, k := range header {
			v[i] = row[k]
		}
		_ = w.Write(v)
	}
	w.Flush()
	if e = w.Error(); e != nil {
		t.Fatal(e)
	}
	if e = f.Close(); e != nil {
		t.Fatal(e)
	}
	return p
}
func fixtures(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	eh := strings.Split("siren,siret,statutDiffusionEtablissement,etablissementSiege,etatAdministratifEtablissement,denominationUsuelleEtablissement,enseigne1Etablissement,enseigne2Etablissement,enseigne3Etablissement,numeroVoieEtablissement,indiceRepetitionEtablissement,typeVoieEtablissement,libelleVoieEtablissement,codePostalEtablissement,libelleCommuneEtablissement,activitePrincipaleEtablissement", ",")
	var er []map[string]string
	for i := 1; i <= 5; i++ {
		diff := "O"
		if i == 5 {
			diff = "P"
		}
		active := "A"
		if i == 4 {
			active = "F"
		}
		er = append(er, map[string]string{"siren": "012345678", "siret": fmt.Sprintf("012345678%05d", i), "statutDiffusionEtablissement": diff, "etatAdministratifEtablissement": active, "etablissementSiege": fmt.Sprint(i == 2), "enseigne1Etablissement": "Boutique, café", "enseigne2Etablissement": "Alias Secret", "numeroVoieEtablissement": "7", "typeVoieEtablissement": "RUE", "libelleVoieEtablissement": "DU PORT", "codePostalEtablissement": "97110", "libelleCommuneEtablissement": "Pointe-à-Pitre", "activitePrincipaleEtablissement": "47.11A"})
	}
	er = append(er, map[string]string{"siren": "987654321", "siret": "98765432100001", "statutDiffusionEtablissement": "O", "etatAdministratifEtablissement": "A", "enseigne1Etablissement": "Private"})
	lh := strings.Split("siren,statutDiffusionUniteLegale,etatAdministratifUniteLegale,denominationUniteLegale,nomUniteLegale,prenom1UniteLegale,denominationUsuelle1UniteLegale,denominationUsuelle2UniteLegale,denominationUsuelle3UniteLegale,sigleUniteLegale,categorieJuridiqueUniteLegale", ",")
	lr := []map[string]string{{"siren": "012345678", "statutDiffusionUniteLegale": "O", "etatAdministratifUniteLegale": "A", "denominationUniteLegale": "Société Étoile", "categorieJuridiqueUniteLegale": "5710"}, {"siren": "987654321", "statutDiffusionUniteLegale": "P", "etatAdministratifUniteLegale": "A", "denominationUniteLegale": "Private"}}
	return fixture(t, dir, "StockEtablissement_utf8.csv", eh, er), fixture(t, dir, "StockUniteLegale_utf8.csv", lh, lr)
}
func request(h http.Handler, path string) *httptest.ResponseRecorder {
	r := httptest.NewRecorder()
	h.ServeHTTP(r, httptest.NewRequest("GET", path, nil))
	return r
}
func TestImportAndAPI(t *testing.T) {
	c := testConfig(t)
	s := &store{cfg: c}
	defer s.close()
	h := s.handler()
	if r := request(h, "/readyz"); r.Code != 503 {
		t.Fatal(r.Code)
	}
	est, legal := fixtures(t)
	if e := s.refresh(context.Background(), est, legal); e != nil {
		t.Fatal(e)
	}
	if s.count != 4 {
		t.Fatalf("privacy filters: got %d rows", s.count)
	}
	r := request(h, "/api/pappers-search?q=etoile&limit=2")
	if r.Code != 200 {
		t.Fatal(r.Body.String())
	}
	var page struct {
		Companies []company `json:"companies"`
		Next      string    `json:"next_cursor"`
	}
	if e := json.Unmarshal(r.Body.Bytes(), &page); e != nil {
		t.Fatal(e)
	}
	if len(page.Companies) != 2 || page.Next == "" {
		t.Fatal(r.Body.String())
	}
	if page.Companies[0].SIRET != "01234567800001" || page.Companies[0].LegalName != "Société Étoile" || page.Companies[0].Address != "7 RUE DU PORT" {
		t.Fatal(page)
	}
	saved := page.Next
	page.Next = ""
	r = request(h, "/api/pappers-search?q=etoile&limit=2&cursor="+url.QueryEscape(saved))
	if r.Code != 200 {
		t.Fatal(r.Body.String())
	}
	if e := json.Unmarshal(r.Body.Bytes(), &page); e != nil {
		t.Fatal(e)
	}
	if len(page.Companies) != 1 || page.Next != "" {
		t.Fatal(r.Body.String())
	}
	for _, tc := range []struct {
		path, contains string
		status         int
	}{
		{"/api/pappers-search?q=012%20345%20678", "01234567800002", 200},
		{"/api/pappers-search?q=01234567800003", "01234567800003", 200},
		{"/api/companies?q=alias%20secret", "01234567800001", 200},
		{"/api/companies?q=%27%20OR%201%3D1--", `"companies":[]`, 200},
		{"/api/pappers-company?siret=01234567800004", `"active":false`, 200},
		{"/api/companies/01234567800005", "not found", 404},
		{"/api/companies/98765432100001", "not found", 404},
		{"/api/pappers-company?siret=x", "SIRET", 400},
		{"/api/companies?q=etoile&cursor=bad", "cursor", 400},
		{"/api/companies?q=etoile&limit=101", "limit", 400},
		{"/api/companies?q=etoile&active=false", "01234567800004", 200},
	} {
		r = request(h, tc.path)
		if r.Code != tc.status || !strings.Contains(r.Body.String(), tc.contains) {
			t.Errorf("%s => %d %s", tc.path, r.Code, r.Body.String())
		}
	}
	// Invalid replacement cannot destroy the serving snapshot.
	version := s.version
	if e := s.refresh(context.Background(), legal, est); e == nil {
		t.Fatal("expected invalid schema failure")
	}
	if s.version != version {
		t.Fatal("failed import replaced registry")
	}
	s2 := &store{cfg: c}
	if e := s2.load(); e != nil {
		t.Fatal(e)
	}
	if s2.count != 4 || s2.version != version {
		t.Fatal("restart lost snapshot")
	}
	s2.close()
	if e := s.refresh(context.Background(), est, legal); e != nil {
		t.Fatal(e)
	}
	r = request(h, "/api/companies?q=etoile&limit=2&cursor="+url.QueryEscape(saved))
	if r.Code != 409 {
		t.Fatal(r.Code, r.Body.String())
	}
	s.cfg.APIKey = "test-token"
	r = request(h, "/api/status")
	if r.Code != 401 {
		t.Fatal(r.Code)
	}
	rr := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/api/status", nil)
	req.Header.Set("Authorization", "Bearer test-token")
	h.ServeHTTP(rr, req)
	if rr.Code != 200 {
		t.Fatal(rr.Code)
	}
	if r = request(h, "/healthz"); r.Code != 200 {
		t.Fatal(r.Code)
	}
}
func makeZip(t *testing.T, path, member, content string) {
	t.Helper()
	f, e := os.Create(path)
	if e != nil {
		t.Fatal(e)
	}
	z := zip.NewWriter(f)
	w, e := z.Create(member)
	if e != nil {
		t.Fatal(e)
	}
	_, _ = w.Write([]byte(content))
	if e = z.Close(); e != nil {
		t.Fatal(e)
	}
	_ = f.Close()
}
func TestDownloadAndExtraction(t *testing.T) {
	dir := t.TempDir()
	archive := filepath.Join(dir, "fixture.zip")
	makeZip(t, archive, "../../StockEtablissement_utf8.csv", "a,b\n1,2\n")
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/redirect" {
			http.Redirect(w, r, "/zip", 302)
			return
		}
		http.ServeFile(w, r, archive)
	}))
	defer srv.Close()
	dest := filepath.Join(dir, "download.zip")
	if e := download(context.Background(), srv.URL+"/redirect", dest, 1<<20); e != nil {
		t.Fatal(e)
	}
	csv := filepath.Join(dir, "fixed.csv")
	if e := extract(context.Background(), dest, csv, "StockEtablissement_utf8.csv", 100); e != nil {
		t.Fatal(e)
	}
	if b, e := os.ReadFile(csv); e != nil || string(b) != "a,b\n1,2\n" {
		t.Fatal(string(b), e)
	}
	if e := extract(context.Background(), dest, filepath.Join(dir, "oversize.csv"), "StockEtablissement_utf8.csv", 2); e == nil {
		t.Fatal("expected extraction limit")
	}
	if e := download(context.Background(), srv.URL+"/zip", filepath.Join(dir, "limited.zip"), 2); e == nil {
		t.Fatal("expected download limit")
	}
	if e := extract(context.Background(), dest, filepath.Join(dir, "missing.csv"), "StockUniteLegale_utf8.csv", 100); e == nil {
		t.Fatal("expected missing member")
	}
}
func TestScheduledRedirectedRefresh(t *testing.T) {
	est, legal := fixtures(t)
	dir := t.TempDir()
	for _, p := range []string{est, legal} {
		b, e := os.ReadFile(p)
		if e != nil {
			t.Fatal(e)
		}
		makeZip(t, filepath.Join(dir, filepath.Base(p)+".zip"), filepath.Base(p), string(b))
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/est" {
			http.Redirect(w, r, "/StockEtablissement_utf8.csv.zip", 307)
			return
		}
		http.ServeFile(w, r, filepath.Join(dir, filepath.Base(r.URL.Path)))
	}))
	defer srv.Close()
	c := testConfig(t)
	c.EstablishmentURL = srv.URL + "/est"
	c.LegalUnitURL = srv.URL + "/StockUniteLegale_utf8.csv.zip"
	s := &store{cfg: c}
	defer s.close()
	bootstrap, stopBootstrap := context.WithTimeout(context.Background(), 5*time.Second)
	done := make(chan struct{})
	go func() { defer close(done); s.schedule(bootstrap) }()
	for {
		s.mu.RLock()
		ready := s.db != nil
		s.mu.RUnlock()
		if ready {
			break
		}
		select {
		case <-bootstrap.Done():
			stopBootstrap()
			<-done
			t.Fatal("scheduled bootstrap timed out")
		case <-time.After(5 * time.Millisecond):
		}
	}
	stopBootstrap()
	<-done
	if s.count != 4 {
		t.Fatal(s.count)
	}
	version := s.version
	c.EstablishmentURL = srv.URL + "/missing"
	s.cfg = c
	if e := s.refresh(context.Background(), "", ""); e == nil {
		t.Fatal("expected HTTP failure")
	}
	if s.version != version {
		t.Fatal("download failure replaced registry")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	s.schedule(ctx)
	if s.version != version {
		t.Fatal("fresh snapshot was downloaded again")
	}
}
