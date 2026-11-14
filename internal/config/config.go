// Package config provides tiny helpers for 12-factor style configuration:
// every command-line flag can default from an environment variable, so the same
// binary runs unchanged under Docker Compose, Kubernetes or a laptop shell.
package config

import (
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// String returns $key or def.
func String(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

// Int returns $key parsed as an int, or def if unset/invalid.
func Int(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

// Float returns $key parsed as a float64, or def.
func Float(key string, def float64) float64 {
	if v, err := strconv.ParseFloat(os.Getenv(key), 64); err == nil {
		return v
	}
	return def
}

// Duration returns $key parsed as a time.Duration, or def.
func Duration(key string, def time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

// List returns $key split on commas (blank entries dropped), or def.
func List(key string, def []string) []string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	var out []string
	for _, p := range strings.Split(v, ",") {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// Logger builds a structured logger writing to stderr. level is one of
// debug|info|warn|error (anything else means info).
func Logger(level string) *slog.Logger {
	var l slog.Level
	switch strings.ToLower(level) {
	case "debug":
		l = slog.LevelDebug
	case "warn":
		l = slog.LevelWarn
	case "error":
		l = slog.LevelError
	}
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: l}))
}
