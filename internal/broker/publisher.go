package broker

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/xid"
)

type Publisher struct {
	conn   *Connection
	ch     *amqp.Channel
	logger *slog.Logger
	mu     sync.Mutex
}

func NewPublisher(conn *Connection, logger *slog.Logger) (*Publisher, error) {
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("creating publisher channel: %w", err)
	}

	// Enable publisher confirms
	if err := ch.Confirm(false); err != nil {
		ch.Close()
		return nil, fmt.Errorf("enabling confirms: %w", err)
	}

	return &Publisher{
		conn:   conn,
		ch:     ch,
		logger: logger,
	}, nil
}

func (p *Publisher) Publish(ctx context.Context, exchange, routingKey string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshalling message: %w", err)
	}

	p.mu.Lock()
	defer p.mu.Unlock()

	msg := amqp.Publishing{
		MessageId:    xid.New().String(),
		Timestamp:    time.Now(),
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		Body:         body,
	}

	deferredConfirm, err := p.ch.PublishWithDeferredConfirmWithContext(ctx, exchange, routingKey, false, false, msg)
	if err != nil {
		return fmt.Errorf("Publishing to %s/%s: %w", exchange, routingKey, err)
	}

	if !deferredConfirm.Wait() {
		return fmt.Errorf("publish not confirmed by broker")
	}

	return nil
}

func (p *Publisher) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.ch != nil {
		return p.ch.Close()
	}
	return nil
}
