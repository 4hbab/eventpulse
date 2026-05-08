package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/4hbab/eventpulse/internal/broker"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load()
	if err != nil {
		slog.Warn("No .env file found, falling back to system environment variables")
	}

	rabbitURL := os.Getenv("RABBITMQ_URL")
	logLevelStr := os.Getenv("LOG_LEVEL")

	var programLevel = slog.LevelInfo
	if logLevelStr == "debug" {
		programLevel = slog.LevelDebug
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: programLevel,
	}))

	conn, err := broker.NewConnection(rabbitURL, logger)
	if err != nil {
		logger.Error("failed to connect", "error", err, "url", rabbitURL)
		os.Exit(1)
	}
	defer conn.Close()

	logger.Info("EventPulse started!", "port", os.Getenv("SERVER_PORT"))
	logger.Info("Press Ctrl+C to exit")

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logger.Info("shutting down...")
}
