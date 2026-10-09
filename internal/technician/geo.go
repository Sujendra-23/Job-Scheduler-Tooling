// Package technician implements field-technician dispatch: assigning work orders to
// technicians by skill and travel time, and re-assigning when jobs are cancelled or a
// technician runs late. It sits beside (not inside) the ML inference dispatcher and reuses
// its telemetry recorder and its terminal-state / dead-letter conventions.
package technician

import (
	"math"
	"time"
)

// Point is a WGS84 coordinate in degrees.
type Point struct {
	Lat float64 `json:"lat"`
	Lng float64 `json:"lng"`
}

const earthRadiusKm = 6371.0088

// HaversineKm returns the great-circle distance between a and b in kilometres.
func HaversineKm(a, b Point) float64 {
	rad := math.Pi / 180
	dLat := (b.Lat - a.Lat) * rad
	dLng := (b.Lng - a.Lng) * rad
	h := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(a.Lat*rad)*math.Cos(b.Lat*rad)*math.Sin(dLng/2)*math.Sin(dLng/2)
	return 2 * earthRadiusKm * math.Asin(math.Min(1, math.Sqrt(h)))
}

// Valid reports whether p is a real coordinate (not NaN, within range).
func (p Point) Valid() bool {
	return !math.IsNaN(p.Lat) && !math.IsNaN(p.Lng) &&
		p.Lat >= -90 && p.Lat <= 90 && p.Lng >= -180 && p.Lng <= 180
}

// Haversine fallback model: straight-line distance inflated by a road-circuity factor and
// driven at an average urban speed. It is a coarse estimate, which is why results carry a
// Source label and why the primary router is preferred.
const (
	DefaultCircuity = 1.3
	DefaultSpeedKmh = 40.0
)

func haversineTravel(a, b Point, circuity, speedKmh float64) time.Duration {
	hours := HaversineKm(a, b) * circuity / speedKmh
	return time.Duration(hours * float64(time.Hour))
}
