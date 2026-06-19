package broker

import (
	"fmt"
	"log/slog"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Defines the full RabbitMQ topology for a simulation
// We're using a struct instead of RAW function params to
// define different Topology types rather than generalizing to a single type
type TopologyConfig struct {
	Exchanges []ExchangeConfig
	Queues    []QueueConfig
	Bindings  []BindingConfig
}

type ExchangeConfig struct {
	Name    string
	Kind    string
	Durable bool
}

type QueueConfig struct {
	Name    string
	Durable bool
	Args    amqp.Table
}

type BindingConfig struct {
	QueueName    string
	ExchangeName string
	RoutingKey   string
}

func SetupTopology(
	ch *amqp.Channel,
	cfg TopologyConfig,
	logger *slog.Logger,
) error {
	for _, ex := range cfg.Exchanges {
		err := ch.ExchangeDeclare(
			ex.Name,
			ex.Kind,
			ex.Durable,
			false,
			false,
			false,
			nil,
		)

		if err != nil {
			return fmt.Errorf("Declaring exchange %s: %w", ex.Name, err)
		}
		logger.Debug("exchange declared", "name", ex.Name, "kind", ex.Kind)
	}

	for _, q := range cfg.Queues {
		_, err := ch.QueueDeclare(
			q.Name,
			q.Durable,
			false,
			false,
			false,
			q.Args,
		)

		if err != nil {
			return fmt.Errorf("Declaring queue %s: %w", q.Name, err)
		}
		logger.Debug("queue declared", "name", q.Name)
	}

	for _, b := range cfg.Bindings {
		err := ch.QueueBind(
			b.QueueName,
			b.RoutingKey,
			b.ExchangeName,
			false,
			nil,
		)

		if err != nil {
			return fmt.Errorf("binding %s to %s: %w", b.QueueName, b.ExchangeName, err)
		}
		logger.Debug("queue bound",
			"queue", b.QueueName,
			"exchange", b.ExchangeName,
			"routing_key", b.RoutingKey,
		)
	}
	logger.Debug("Topology created")

	return nil
}

func TeardownTopology(ch *amqp.Channel, cfg TopologyConfig, logger *slog.Logger) error {
	for _, q := range cfg.Queues {
		_, err := ch.QueueDelete(q.Name, false, false, false)
		if err != nil {
			logger.Warn("failed to delete queue", "name", q.Name, "error", err)
		}
	}
	for _, ex := range cfg.Exchanges {
		err := ch.ExchangeDelete(ex.Name, false, false)
		if err != nil {
			logger.Warn("failed to delete exchange", "name", ex.Name, "error", err)
		}
	}
	logger.Debug("topology torn down")

	return nil
}
