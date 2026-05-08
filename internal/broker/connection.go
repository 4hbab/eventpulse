package broker

import (
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// AMQP wrapper for auto-reconnect capability
type Connection struct {
	url    string
	conn   *amqp.Connection
	mu     sync.RWMutex // protects conn from concurrent access
	logger *slog.Logger
	closed chan struct{}
}

func NewConnection(url string, logger *slog.Logger) (*Connection, error) {
	c := &Connection{
		url:    url,
		logger: logger,
		closed: make(chan struct{}),
	}

	if err := c.connect(); err != nil {
		return nil, fmt.Errorf("Initial connection failed: %w", err)
	}

	go c.handleReconnect()

	return c, nil
}

func (c *Connection) connect() error {
	conn, err := amqp.Dial(c.url)
	if err != nil {
		return fmt.Errorf("Failed to dial RabbitMQ: %w", err)
	}

	c.mu.Lock()
	c.conn = conn
	c.mu.Unlock()

	c.logger.Info("Connected to RabbitMQ", "url", c.url)
	return nil
}

func (c *Connection) handleReconnect() {
	for {
		c.mu.RLock()
		conn := c.conn
		c.mu.RUnlock()

		if conn == nil {
			return
		}

		notifyClose := conn.NotifyClose(make(chan *amqp.Error, 1))

		select {
		case <-c.closed:
			return
		case err := <-notifyClose:
			if err != nil {
				c.logger.Error("Connection lost", "error", err)
			}
		}

		c.reconnectWithBackoff()
	}
}

func (c *Connection) reconnectWithBackoff() {
	backOff := time.Second
	maxBackOff := 30 * time.Second

	for attempt := 1; ; attempt++ {
		select {
		case <-c.closed:
			return
		default:
		}

		c.logger.Info("attempting reconnect",
			"attempt", attempt,
			"backoff", backOff,
		)

		if err := c.connect(); err != nil {
			c.logger.Error("reconnect failed",
				"attempt", attempt,
				"error", err,
				"next_retry_in", backOff,
			)

			select {
			case <-time.After(backOff):
				// time to retry
			case <-c.closed:
				return
			}

			backOff *= 2
			if backOff > maxBackOff {
				backOff = maxBackOff
			}
			continue
		}

		c.logger.Info("reconnected successfully", "attempt", attempt)
		return
	}
}

// Creates a new AMQP channel from the TCP connection
func (c *Connection) Channel() (*amqp.Channel, error) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	if c.conn == nil || c.conn.IsClosed() {
		return nil, fmt.Errorf("connection is closed")
	}

	ch, err := c.conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("failed to open channel: %w", err)
	}

	return ch, nil
}

func (c *Connection) Close() error {
	close(c.closed) // signal reconnecting goroutine to stop

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.conn != nil && !c.conn.IsClosed() {
		return c.conn.Close()
	}

	return nil
}
