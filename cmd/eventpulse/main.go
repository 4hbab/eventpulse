package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/4hbab/eventpulse/internal/broker"
	"github.com/4hbab/eventpulse/internal/event"
	"github.com/4hbab/eventpulse/internal/simulation"
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

	// Setup connection
	conn, err := broker.NewConnection(rabbitURL, logger)
	if err != nil {
		logger.Error("failed to connect", "error", err, "url", rabbitURL)
		os.Exit(1)
	}
	defer conn.Close()

	logger.Info("EventPulse started!", "port", os.Getenv("SERVER_PORT"))
	logger.Info("Press Ctrl+C to exit")

	// Setup topology
	ch, _ := conn.Channel()
	topo := broker.TopologyConfig{
		Exchanges: []broker.ExchangeConfig{
			{Name: "events.topic", Kind: "topic", Durable: true},
		},
		Queues: []broker.QueueConfig{
			{Name: "test-queue", Durable: true, Args: nil},
		},
		Bindings: []broker.BindingConfig{
			{QueueName: "test-queue", ExchangeName: "events.topic", RoutingKey: "event.#"},
		},
	}
	if err := broker.SetupTopology(ch, topo, logger); err != nil {
		logger.Error("topology setup failed", "error", err)
		os.Exit(1)
	}
	ch.Close()

	// Create publisher + generator
	pub, _ := broker.NewPublisher(conn, logger)
	defer pub.Close()

	gen := simulation.NewGenerator(pub, "events.topic", event.AllTypes(), logger)

	// Run generator
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		gen.Start(ctx, 5)
	}()

	logger.Info("publishing 5 events/sec - check RabbitMQ UI")

	// Print stats every 3 seconds
	go func() {
		for {
			time.Sleep(3 * time.Second)
			pub, errs := gen.Stats()
			logger.Info("stats", "published", pub, "errors", errs)
		}
	}()

	// Wait for shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	cancel()
	logger.Info("shutting down...")
}
