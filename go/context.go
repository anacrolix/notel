package telemetry

import (
	"context"
	"log/slog"
)

type sloggerKey struct{}

// Adds the slog.Logger into the context. Presumably the slog.Logger is used for telemetry.
func ContextWithSlogger(ctx context.Context, l *slog.Logger) context.Context {
	return context.WithValue(ctx, sloggerKey{}, l)
}

func SloggerFromContext(ctx context.Context) *slog.Logger {
	return ctx.Value(sloggerKey{}).(*slog.Logger)
}

func MustSloggerFromContext(ctx context.Context) *slog.Logger {
	slogger := SloggerFromContext(ctx)
	if slogger == nil {
		panic("where banana")
	}
	return slogger
}
