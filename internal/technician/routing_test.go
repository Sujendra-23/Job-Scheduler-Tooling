package technician

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"
)

func TestHaversineKnownDistances(t *testing.T) {
	nyc, la := Point{40.7128, -74.0060}, Point{34.0522, -118.2437}
	if got := HaversineKm(nyc, la); math.Abs(got-3936) > 15 {
		t.Fatalf("NYC-LA = %.1f km, want ~3936", got)
	}
	if got := HaversineKm(nyc, nyc); got != 0 {
		t.Fatalf("same point = %v, want 0", got)
	}
	if a, b := HaversineKm(nyc, la), HaversineKm(la, nyc); math.Abs(a-b) > 1e-9 {
		t.Fatalf("not symmetric: %v vs %v", a, b)
	}
}

func TestPointValid(t *testing.T) {
	for _, p := range []Point{{91, 0}, {-91, 0}, {0, 181}, {0, -181}, {math.NaN(), 0}, {0, math.NaN()}} {
		if p.Valid() {
			t.Errorf("%+v should be invalid", p)
		}
	}
	if !(Point{30.27, -97.74}).Valid() {
		t.Error("Austin should be valid")
	}
}

func TestHaversineRouterAppliesCircuityAndSpeed(t *testing.T) {
	origin := Point{0, 0}
	dest := Point{0, 0.08993} // ~10 km east at the equator
	m, err := HaversineRouter{}.TravelTimes(context.Background(), []Point{origin}, []Point{dest})
	if err != nil || m.Source != SourceHaversine {
		t.Fatalf("err=%v source=%q", err, m.Source)
	}
	// 10 km * 1.3 / 40 km/h = 19.5 min
	if got := m.Durations[0][0]; got < 19*time.Minute || got > 20*time.Minute {
		t.Fatalf("duration = %v, want ~19.5m", got)
	}
	fast, _ := HaversineRouter{Circuity: 1, SpeedKmh: 60}.TravelTimes(context.Background(), []Point{origin}, []Point{dest})
	if got := fast.Durations[0][0]; got < 9*time.Minute || got > 11*time.Minute {
		t.Fatalf("custom model duration = %v, want ~10m", got)
	}
}

func osrmServer(t *testing.T, handler http.HandlerFunc) OSRMRouter {
	t.Helper()
	srv := httptest.NewServer(handler)
	t.Cleanup(srv.Close)
	return OSRMRouter{BaseURL: srv.URL}
}

func TestOSRMRouterBuildsTableRequestAndParsesDurations(t *testing.T) {
	var gotPath, gotQuery string
	router := osrmServer(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath, gotQuery = r.URL.Path, r.URL.Query().Encode()
		w.Write([]byte(`{"code":"Ok","durations":[[214.2,60],[573.2,0]]}`))
	})
	origins := []Point{{30.2672, -97.7431}, {30.3, -97.7}}
	dests := []Point{{30.28, -97.75}, {30.31, -97.71}}
	m, err := router.TravelTimes(context.Background(), origins, dests)
	if err != nil {
		t.Fatal(err)
	}
	// OSRM takes lng,lat; sources/destinations index into the combined coordinate list.
	wantPath := "/table/v1/driving/-97.743100,30.267200;-97.700000,30.300000;-97.750000,30.280000;-97.710000,30.310000"
	if gotPath != wantPath {
		t.Fatalf("path = %s\nwant   %s", gotPath, wantPath)
	}
	for _, want := range []string{"sources=0%3B1", "destinations=2%3B3", "annotations=duration"} {
		if !strings.Contains(gotQuery, want) {
			t.Errorf("query %q missing %q", gotQuery, want)
		}
	}
	if m.Source != SourceOSRM || m.Durations[0][0] != time.Duration(214.2*float64(time.Second)) ||
		m.Durations[0][1] != time.Minute || m.Durations[1][1] != 0 {
		t.Fatalf("unexpected matrix: %+v", m)
	}
}

func TestOSRMRouterErrors(t *testing.T) {
	ctx := context.Background()
	one, two := []Point{{1, 1}}, []Point{{2, 2}}
	cases := map[string]struct {
		handler http.HandlerFunc
		wantErr error
		wantMsg string
	}{
		"http 500":      {func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) }, nil, "HTTP 500"},
		"http 429":      {func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(429) }, nil, "HTTP 429"},
		"bad json":      {func(w http.ResponseWriter, r *http.Request) { w.Write([]byte("<html>")) }, nil, "bad JSON"},
		"code not ok":   {func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{"code":"InvalidQuery"}`)) }, nil, `code "InvalidQuery"`},
		"null duration": {func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{"code":"Ok","durations":[[null]]}`)) }, ErrUnroutable, ""},
		"wrong rows":    {func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{"code":"Ok","durations":[]}`)) }, nil, "rows"},
		"wrong columns": {func(w http.ResponseWriter, r *http.Request) { w.Write([]byte(`{"code":"Ok","durations":[[1,2]]}`)) }, nil, "columns"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := osrmServer(t, tc.handler).TravelTimes(ctx, one, two)
			if err == nil {
				t.Fatal("expected an error")
			}
			if tc.wantErr != nil && !errors.Is(err, tc.wantErr) {
				t.Fatalf("err = %v, want %v", err, tc.wantErr)
			}
			if tc.wantMsg != "" && !strings.Contains(err.Error(), tc.wantMsg) {
				t.Fatalf("err = %v, want to contain %q", err, tc.wantMsg)
			}
		})
	}
	if _, err := (OSRMRouter{BaseURL: "http://unused"}).TravelTimes(ctx, []Point{{999, 0}}, two); err == nil {
		t.Fatal("invalid coordinates must be rejected before any request")
	}
	if m, err := (OSRMRouter{BaseURL: "http://unused"}).TravelTimes(ctx, nil, two); err != nil || len(m.Durations) != 0 {
		t.Fatalf("empty origins: %v %v", m, err)
	}
}

func TestOSRMRouterHonoursContextDeadline(t *testing.T) {
	router := osrmServer(t, func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-r.Context().Done():
		case <-time.After(2 * time.Second):
		}
	})
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	start := time.Now()
	if _, err := router.TravelTimes(ctx, []Point{{1, 1}}, []Point{{2, 2}}); err == nil {
		t.Fatal("expected a timeout error")
	}
	if time.Since(start) > time.Second {
		t.Fatal("request did not respect the context deadline")
	}
}

type failingRouter struct{ err error }

func (f failingRouter) TravelTimes(context.Context, []Point, []Point) (Matrix, error) {
	return Matrix{}, f.err
}

func TestFallbackRouter(t *testing.T) {
	ctx := context.Background()
	origins, dests := []Point{{30, -97}}, []Point{{30.1, -97.1}}

	var seen error
	ok := FallbackRouter{Primary: HaversineRouter{SpeedKmh: 10}, Fallback: HaversineRouter{}, OnFallback: func(e error) { seen = e }}
	m, err := ok.TravelTimes(ctx, origins, dests)
	if err != nil || m.Source != SourceHaversine || seen != nil {
		t.Fatalf("primary success path wrong: %v %q %v", err, m.Source, seen)
	}

	boom := errors.New("osrm down")
	fb := FallbackRouter{Primary: failingRouter{boom}, Fallback: HaversineRouter{}, OnFallback: func(e error) { seen = e }}
	m, err = fb.TravelTimes(ctx, origins, dests)
	if err != nil || m.Source != SourceFallback || !errors.Is(seen, boom) || m.Durations[0][0] <= 0 {
		t.Fatalf("fallback path wrong: %v %+v %v", err, m, seen)
	}

	both := FallbackRouter{Primary: failingRouter{boom}, Fallback: failingRouter{errors.New("also down")}}
	if _, err := both.TravelTimes(ctx, origins, dests); err == nil || !strings.Contains(err.Error(), "also down") {
		t.Fatalf("expected combined error, got %v", err)
	}
}

// TestOSRMLive talks to a real OSRM server. It is skipped unless OSRM_URL is set, so the
// suite never needs network access; run it by hand against your own OSRM instance (or,
// sparingly, the public demo server) to check the request and response shapes.
func TestOSRMLive(t *testing.T) {
	url := os.Getenv("OSRM_URL")
	if url == "" {
		t.Skip("OSRM_URL not set; skipping live OSRM check")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	m, err := OSRMRouter{BaseURL: url}.TravelTimes(ctx,
		[]Point{{30.2672, -97.7431}, {30.3, -97.7}}, []Point{{30.28, -97.75}})
	if err != nil {
		t.Fatal(err)
	}
	if len(m.Durations) != 2 || m.Durations[0][0] <= 0 || m.Durations[0][0] > time.Hour {
		t.Fatalf("implausible live result: %+v", m)
	}
	t.Logf("live OSRM durations: %v", m.Durations)
}
