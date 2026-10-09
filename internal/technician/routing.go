package technician

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Travel sources recorded with every assignment so a reader knows how an ETA was derived.
const (
	SourceOSRM      = "osrm"
	SourceHaversine = "haversine"
	// SourceFallback means the primary router failed and haversine was used instead.
	SourceFallback = "haversine-fallback"
)

// Matrix is a travel-time matrix: Durations[i][j] is origin i -> destination j.
type Matrix struct {
	Durations [][]time.Duration
	Source    string
}

// Router computes travel times. Implementations must be safe for concurrent use.
type Router interface {
	TravelTimes(ctx context.Context, origins, dests []Point) (Matrix, error)
}

// HaversineRouter needs no network: straight-line distance scaled to road time.
type HaversineRouter struct {
	Circuity float64 // 0 = DefaultCircuity
	SpeedKmh float64 // 0 = DefaultSpeedKmh
}

func (h HaversineRouter) TravelTimes(_ context.Context, origins, dests []Point) (Matrix, error) {
	circuity, speed := h.Circuity, h.SpeedKmh
	if circuity <= 0 {
		circuity = DefaultCircuity
	}
	if speed <= 0 {
		speed = DefaultSpeedKmh
	}
	m := Matrix{Source: SourceHaversine, Durations: make([][]time.Duration, len(origins))}
	for i, o := range origins {
		m.Durations[i] = make([]time.Duration, len(dests))
		for j, d := range dests {
			m.Durations[i][j] = haversineTravel(o, d, circuity, speed)
		}
	}
	return m, nil
}

// OSRMRouter calls an OSRM server's Table service (/table/v1/driving), the same OSRM routing
// the sibling ai_maps project uses for route geometry. Point it at your own OSRM instance for
// real use; the public demo server (https://router.project-osrm.org) is rate limited and meant
// for light testing only.
type OSRMRouter struct {
	BaseURL string       // e.g. https://router.project-osrm.org
	Client  *http.Client // nil = a client with a 3 s timeout
}

// ErrUnroutable is returned when OSRM cannot connect a pair of points.
var ErrUnroutable = errors.New("osrm: no route between some points")

type osrmTable struct {
	Code      string       `json:"code"`
	Message   string       `json:"message"`
	Durations [][]*float64 `json:"durations"`
}

func (o OSRMRouter) TravelTimes(ctx context.Context, origins, dests []Point) (Matrix, error) {
	if len(origins) == 0 || len(dests) == 0 {
		return Matrix{Source: SourceOSRM}, nil
	}
	coords := make([]string, 0, len(origins)+len(dests))
	for _, p := range append(append([]Point{}, origins...), dests...) {
		if !p.Valid() {
			return Matrix{}, fmt.Errorf("osrm: invalid coordinate %+v", p)
		}
		// OSRM wants lng,lat.
		coords = append(coords, strconv.FormatFloat(p.Lng, 'f', 6, 64)+","+strconv.FormatFloat(p.Lat, 'f', 6, 64))
	}
	idx := func(from, n int) string {
		parts := make([]string, n)
		for i := range parts {
			parts[i] = strconv.Itoa(from + i)
		}
		return strings.Join(parts, ";")
	}
	q := url.Values{}
	q.Set("sources", idx(0, len(origins)))
	q.Set("destinations", idx(len(origins), len(dests)))
	q.Set("annotations", "duration")
	endpoint := strings.TrimRight(o.BaseURL, "/") + "/table/v1/driving/" + strings.Join(coords, ";") + "?" + q.Encode()

	client := o.Client
	if client == nil {
		client = &http.Client{Timeout: 3 * time.Second}
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return Matrix{}, err
	}
	resp, err := client.Do(req)
	if err != nil {
		return Matrix{}, fmt.Errorf("osrm: %w", err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	if err != nil {
		return Matrix{}, fmt.Errorf("osrm: reading response: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return Matrix{}, fmt.Errorf("osrm: HTTP %d", resp.StatusCode)
	}
	var table osrmTable
	if err := json.Unmarshal(body, &table); err != nil {
		return Matrix{}, fmt.Errorf("osrm: bad JSON: %w", err)
	}
	if table.Code != "Ok" {
		return Matrix{}, fmt.Errorf("osrm: code %q", table.Code)
	}
	if len(table.Durations) != len(origins) {
		return Matrix{}, fmt.Errorf("osrm: expected %d rows, got %d", len(origins), len(table.Durations))
	}
	m := Matrix{Source: SourceOSRM, Durations: make([][]time.Duration, len(origins))}
	for i, row := range table.Durations {
		if len(row) != len(dests) {
			return Matrix{}, fmt.Errorf("osrm: expected %d columns, got %d", len(dests), len(row))
		}
		m.Durations[i] = make([]time.Duration, len(row))
		for j, seconds := range row {
			if seconds == nil {
				return Matrix{}, ErrUnroutable
			}
			m.Durations[i][j] = time.Duration(*seconds * float64(time.Second))
		}
	}
	return m, nil
}

// FallbackRouter tries Primary and, if it errors, answers with Fallback instead so dispatch
// never stalls on a routing outage. Results are labelled SourceFallback in that case.
type FallbackRouter struct {
	Primary  Router
	Fallback Router
	// OnFallback, if set, is told why the primary was skipped (for logging/metrics).
	OnFallback func(err error)
}

func (f FallbackRouter) TravelTimes(ctx context.Context, origins, dests []Point) (Matrix, error) {
	m, err := f.Primary.TravelTimes(ctx, origins, dests)
	if err == nil {
		return m, nil
	}
	if f.OnFallback != nil {
		f.OnFallback(err)
	}
	m, ferr := f.Fallback.TravelTimes(ctx, origins, dests)
	if ferr != nil {
		return Matrix{}, fmt.Errorf("primary: %v; fallback: %w", err, ferr)
	}
	m.Source = SourceFallback
	return m, nil
}
