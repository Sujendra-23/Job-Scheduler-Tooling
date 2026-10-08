package devices

import (
	"encoding/json"
	"net/http"
	"strings"
)

// Handler exposes registry status and config updates over HTTP:
//
//	GET /devices            -> all device statuses
//	GET /devices/{id}       -> one device
//	PUT /devices/{id}/config  body: {"key":"value",...} -> 202 {"version":N}
func Handler(r *Registry) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/devices", func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		writeJSON(w, http.StatusOK, r.List())
	})
	mux.HandleFunc("/devices/", func(w http.ResponseWriter, req *http.Request) {
		rest := strings.TrimPrefix(req.URL.Path, "/devices/")
		id, sub, _ := strings.Cut(rest, "/")
		switch {
		case req.Method == http.MethodGet && sub == "":
			s, ok := r.Status(id)
			if !ok {
				http.Error(w, "unknown device", http.StatusNotFound)
				return
			}
			writeJSON(w, http.StatusOK, s)
		case req.Method == http.MethodPut && sub == "config":
			var cfg map[string]string
			if err := json.NewDecoder(req.Body).Decode(&cfg); err != nil {
				http.Error(w, "body must be a JSON object of string values", http.StatusBadRequest)
				return
			}
			v, err := r.SetConfig(id, cfg)
			if err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			writeJSON(w, http.StatusAccepted, map[string]int64{"version": v})
		case req.Method == http.MethodPost && sub == "retry":
			if err := r.Retry(id); err != nil {
				http.Error(w, err.Error(), http.StatusNotFound)
				return
			}
			w.WriteHeader(http.StatusAccepted)
		default:
			http.Error(w, "not found", http.StatusNotFound)
		}
	})
	return mux
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
