package simulation

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand"
	"sync/atomic"
	"time"

	"github.com/4hbab/eventpulse/internal/broker"
	"github.com/4hbab/eventpulse/internal/event"
	"github.com/rs/xid"
)

type Generator struct {
	publisher  *broker.Publisher
	exchange   string
	eventTypes []event.EventType
	logger     *slog.Logger

	published atomic.Int64
	errors    atomic.Int64
}

func NewGenerator(pub *broker.Publisher, exchange string, types []event.EventType, logger *slog.Logger) *Generator {
	return &Generator{
		publisher:  pub,
		exchange:   exchange,
		eventTypes: types,
		logger:     logger,
	}
}

func (g *Generator) Start(ctx context.Context, rate int) error {
	if rate <= 0 {
		return fmt.Errorf("rate must be positive, got %d", rate)
	}
	interval := time.Second / time.Duration(rate)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	g.logger.Info("generator started", "rate", rate, "exchange", g.exchange, "interval", interval)

	for {
		select {
		case <-ctx.Done():
			g.logger.Info("generator stopped", "published", g.published.Load(), "errors", g.errors.Load())
			return nil
		case <-ticker.C:
			if err := g.publishOne(ctx); err != nil {
				g.errors.Add(1)
				g.logger.Error("publish failed", "error", err)
			} else {
				g.published.Add(1)
			}
		}
	}
}

func (g *Generator) publishOne(ctx context.Context) error {
	eventType := g.eventTypes[rand.Intn(len(g.eventTypes))]

	evt := event.TrackingEvent{
		ID:        xid.New().String(),
		Type:      eventType,
		UserID:    fmt.Sprintf("user-%d", rand.Intn(1000)),
		SessionID: fmt.Sprintf("sess-%d", rand.Intn(100)),
		Page:      randomPage(),
		Timestamp: time.Now(),
		Metadata:  randomMetadata(eventType),
	}

	return g.publisher.Publish(ctx, g.exchange, string(eventType), evt)
}

func (g *Generator) Stats() (published, errors int64) {
	return g.published.Load(), g.errors.Load()
}

var pages = []string{
	"/home", "/products", "/products/123", "/cart",
	"/checkout", "/about", "/blog", "/blog/go-pubsub",
	"/signup", "/login", "/dashboard", "/settings",
}

func randomPage() string {
	return pages[rand.Intn(len(pages))]
}

func randomMetadata(t event.EventType) map[string]any {
	switch t {
	case event.Purchase:
		return map[string]any{
			"amount":   rand.Float64() * 200,
			"currency": "USD",
			"items":    rand.Intn(5) + 1,
		}
	case event.Click:
		return map[string]any{
			"element": []string{"button", "link", "image", "nav"}[rand.Intn(4)],
			"x":       rand.Intn(1920),
			"y":       rand.Intn(1080),
		}
	case event.Signup:
		return map[string]any{
			"method": []string{"email", "google", "github"}[rand.Intn(3)],
		}
	default:
		return nil
	}
}
