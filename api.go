package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"unicode"
)

type company struct {
	Name          string `json:"name"`
	LegalName     string `json:"legal_name"`
	TradeName     string `json:"trade_name"`
	SIRET         string `json:"siret"`
	SIREN         string `json:"siren"`
	TVAID         string `json:"tva_id"`
	Address       string `json:"address"`
	PostalCode    string `json:"postal_code"`
	City          string `json:"city"`
	LegalForm     string `json:"legal_form"`
	Activity      string `json:"activity"`
	Headquarters  bool   `json:"headquarters"`
	Active        bool   `json:"active"`
	DetailsLoaded bool   `json:"details_loaded"`
}
type cursor struct{ Version, Query, After string }

var identifierRE = regexp.MustCompile(`^(?:[0-9]{9}|[0-9]{14})$`)
var siretRE = regexp.MustCompile(`^[0-9]{14}$`)

func jsonResponse(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.Header().Set("Cache-Control", "no-store")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func apiError(w http.ResponseWriter, status int, msg string) {
	jsonResponse(w, status, map[string]string{"error": msg})
}
func normalizeInput(q string) string {
	q = strings.TrimSpace(q)
	numeric := true
	for _, r := range q {
		if !unicode.IsSpace(r) && (r < '0' || r > '9') {
			numeric = false
			break
		}
	}
	if numeric {
		q = strings.Map(func(r rune) rune {
			if unicode.IsSpace(r) {
				return -1
			}
			return r
		}, q)
	}
	return q
}
func (s *store) handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) { jsonResponse(w, 200, map[string]string{"status": "ok"}) })
	mux.HandleFunc("GET /readyz", func(w http.ResponseWriter, r *http.Request) {
		s.mu.RLock()
		ready := s.db != nil
		s.mu.RUnlock()
		if !ready {
			apiError(w, 503, "initial import pending")
			return
		}
		jsonResponse(w, 200, map[string]string{"status": "ready"})
	})
	mux.HandleFunc("GET /api/status", func(w http.ResponseWriter, r *http.Request) {
		s.mu.RLock()
		defer s.mu.RUnlock()
		jsonResponse(w, 200, map[string]any{"ready": s.db != nil, "refreshing": s.refreshing, "last_success": s.refreshed, "next_refresh": s.refreshed.Add(s.cfg.RefreshInterval), "establishments": s.count, "snapshot": s.version, "last_error": s.lastError})
	})
	mux.HandleFunc("GET /api/companies", s.search)
	mux.HandleFunc("GET /api/pappers-search", s.search)
	mux.HandleFunc("GET /api/companies/{siret}", s.detail)
	mux.HandleFunc("GET /api/pappers-company", s.detail)
	slots := make(chan struct{}, s.cfg.QueryConcurrency)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/api/") {
			if s.cfg.APIKey != "" {
				want := sha256.Sum256([]byte("Bearer " + s.cfg.APIKey))
				got := sha256.Sum256([]byte(r.Header.Get("Authorization")))
				if subtle.ConstantTimeCompare(want[:], got[:]) != 1 {
					apiError(w, 401, "authentication required")
					return
				}
			}
			select {
			case slots <- struct{}{}:
				defer func() { <-slots }()
			default:
				w.Header().Set("Retry-After", "1")
				apiError(w, 429, "too many concurrent requests")
				return
			}
			ctx, cancel := context.WithTimeout(r.Context(), s.cfg.QueryTimeout)
			defer cancel()
			r = r.WithContext(ctx)
		}
		mux.ServeHTTP(w, r)
	})
}

const selectCompany = `SELECT name,legal_name,trade_name,siret,siren,address,postal_code,city,legal_form,activity,headquarters,active FROM companies `

func (s *store) query(ctx context.Context, where, order string, args ...any) ([]company, error) {
	rows, e := s.db.QueryContext(ctx, selectCompany+where+order, args...)
	if e != nil {
		return nil, e
	}
	defer rows.Close()
	out := []company{}
	for rows.Next() {
		c := company{DetailsLoaded: true}
		if e = rows.Scan(&c.Name, &c.LegalName, &c.TradeName, &c.SIRET, &c.SIREN, &c.Address, &c.PostalCode, &c.City, &c.LegalForm, &c.Activity, &c.Headquarters, &c.Active); e != nil {
			return nil, e
		}
		out = append(out, c)
	}
	return out, rows.Err()
}
func queryFailure(w http.ResponseWriter, ctx context.Context, e error) {
	slog.Error("registry query failed", "error", e)
	if ctx.Err() != nil {
		apiError(w, 504, "query deadline exceeded")
	} else {
		apiError(w, 500, "registry query failed")
	}
}
func (s *store) search(w http.ResponseWriter, r *http.Request) {
	q := normalizeInput(r.URL.Query().Get("q"))
	if len(q) > 256 {
		apiError(w, 400, "query too long")
		return
	}
	limit := 50
	if raw := r.URL.Query().Get("limit"); raw != "" {
		n, e := strconv.Atoi(raw)
		if e != nil || n < 1 || n > 100 {
			apiError(w, 400, "limit must be 1..100")
			return
		}
		limit = n
	}
	active := r.URL.Query().Get("active")
	if active != "" && active != "true" && active != "false" {
		apiError(w, 400, "active must be true or false")
		return
	}
	// active=false includes closed establishments, matching detail availability.
	activeOnly := active != "false"
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.db == nil {
		apiError(w, 503, "initial import pending")
		return
	}
	companies := []company{}
	var next any
	if len([]rune(q)) < 3 {
		jsonResponse(w, 200, map[string]any{"companies": companies, "next_cursor": nil})
		return
	}
	where := "WHERE true"
	args := []any{}
	order := " ORDER BY siret LIMIT ?"
	if activeOnly {
		where += " AND active"
	}
	identity := fmt.Sprintf("%s|%d|%t", q, limit, activeOnly)
	after := ""
	if raw := r.URL.Query().Get("cursor"); raw != "" && raw != "*" {
		var c cursor
		b, e := base64.RawURLEncoding.DecodeString(raw)
		if len(raw) > 2048 || e != nil || json.Unmarshal(b, &c) != nil || c.Query != identity || !siretRE.MatchString(c.After) {
			apiError(w, 400, "invalid cursor")
			return
		}
		if c.Version != s.version {
			apiError(w, 409, "registry refreshed; restart search without cursor")
			return
		}
		after = c.After
	}
	if identifierRE.MatchString(q) {
		if len(q) == 14 {
			where += " AND siret=?"
		} else {
			where += " AND siren=?"
			order = " ORDER BY headquarters DESC,siret LIMIT ?"
		}
		args = append(args, q)
		limit = 1
	} else {
		numeric := true
		for _, r := range q {
			if r < '0' || r > '9' {
				numeric = false
				break
			}
		}
		if numeric {
			jsonResponse(w, 200, map[string]any{"companies": companies, "next_cursor": nil})
			return
		}
		where += " AND contains(search_name, lower(strip_accents(?))) AND siret>?"
		args = append(args, q, after)
	}
	args = append(args, limit+1)
	companies, e := s.query(r.Context(), where, order, args...)
	if e != nil {
		queryFailure(w, r.Context(), e)
		return
	}
	if len(companies) > limit {
		companies = companies[:limit]
		if !identifierRE.MatchString(q) {
			b, _ := json.Marshal(cursor{s.version, identity, companies[len(companies)-1].SIRET})
			next = base64.RawURLEncoding.EncodeToString(b)
		}
	}
	jsonResponse(w, 200, map[string]any{"companies": companies, "next_cursor": next})
}
func (s *store) detail(w http.ResponseWriter, r *http.Request) {
	id := r.PathValue("siret")
	if id == "" {
		id = normalizeInput(r.URL.Query().Get("siret"))
	}
	if !siretRE.MatchString(id) {
		apiError(w, 400, "valid 14-digit SIRET required")
		return
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.db == nil {
		apiError(w, 503, "initial import pending")
		return
	}
	rows, e := s.query(r.Context(), "WHERE siret=?", "", id)
	if e != nil {
		queryFailure(w, r.Context(), e)
		return
	}
	if len(rows) == 0 {
		apiError(w, 404, "company not found")
		return
	}
	jsonResponse(w, 200, map[string]any{"company": rows[0]})
}
