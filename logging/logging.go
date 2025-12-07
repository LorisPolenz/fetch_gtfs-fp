package logging

import (
	"log/slog"
	"os"
)

func InitLogger(level string) {
	var logLevel slog.Level

	if level == "debug" {
		logLevel = slog.LevelDebug
	}

	if level == "info" {
		logLevel = slog.LevelInfo
	}

	if level == "warn" {
		logLevel = slog.LevelWarn
	}

	if level == "error" {
		logLevel = slog.LevelError
	}

	// Custom handler to format output
	handler := slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{
		Level:     logLevel, // minimum level
		AddSource: true,     // include file + line

	})
	logger := slog.New(handler)

	slog.SetDefault(logger)
}
