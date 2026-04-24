package testutil

import (
	"context"
	"log/slog"
)

type MockLogHandler struct {
	Records []slog.Record
}

func (h *MockLogHandler) Enabled(_ context.Context, _ slog.Level) bool { return true }
func (h *MockLogHandler) Handle(_ context.Context, r slog.Record) error {
	h.Records = append(h.Records, r)
	return nil
}
func (h *MockLogHandler) WithAttrs(attrs []slog.Attr) slog.Handler { return h }
func (h *MockLogHandler) WithGroup(name string) slog.Handler       { return h }
