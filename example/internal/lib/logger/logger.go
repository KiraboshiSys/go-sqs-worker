package logger

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"os"
	"runtime"
)

type Level slog.Level

const (
	LevelDebug = Level(slog.LevelDebug)
	LevelInfo  = Level(slog.LevelInfo)
	LevelWarn  = Level(slog.LevelWarn)
	LevelError = Level(slog.LevelError)
)

var _logger *slog.Logger

func init() {
	_logger = slog.New(handler())
}

func handler() *slog.JSONHandler {
	var opts = &slog.HandlerOptions{
		Level: slog.Level(LevelDebug),
	}
	mw := io.MultiWriter(os.Stdout)
	return slog.NewJSONHandler(mw, opts)
}

func handle(level slog.Level, msg string, args ...any) {
	_, f, l, _ := runtime.Caller(2)
	source := fmt.Sprintf("%s:%d", f, l)

	args = append(args, slog.String("source", source))

	_logger.Log(context.Background(), level, msg, args...)
}

func Debug(msg string, args ...any) {
	handle(slog.LevelDebug, msg, args...)
}

func Info(msg string, args ...any) {
	handle(slog.LevelInfo, msg, args...)
}

func Warn(msg string, args ...any) {
	handle(slog.LevelWarn, msg, args...)
}

func Error(msg string, args ...any) {
	handle(slog.LevelError, msg, args...)
}
