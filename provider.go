package rmq

import (
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// ReconnectConfig holds configuration for the exponential backoff reconnection logic.
type ReconnectConfig struct {
	// InitialDelay is the first backoff duration before reconnecting.
	InitialDelay time.Duration
	// MaxDelay is the upper bound for exponential backoff.
	MaxDelay time.Duration
	// Multiplier is applied to the current delay on each failed attempt.
	Multiplier float64
	// Heartbeat is the AMQP heartbeat interval negotiated with the broker.
	Heartbeat time.Duration
	// DialTimeout is the maximum time allowed to establish a TCP connection.
	DialTimeout time.Duration
}

func (c *ReconnectConfig) defaults() {
	if c.InitialDelay == 0 {
		c.InitialDelay = time.Second
	}
	if c.MaxDelay == 0 {
		c.MaxDelay = 30 * time.Second
	}
	if c.Multiplier == 0 {
		c.Multiplier = 2.0
	}
	if c.Heartbeat == 0 {
		c.Heartbeat = 10 * time.Second
	}
	if c.DialTimeout == 0 {
		c.DialTimeout = 5 * time.Second
	}
}

// QueueConfig holds queue declaration options.
type QueueConfig struct {
	Name string
	// Durable queues survive broker restarts.
	Durable bool
	// AutoDelete removes the queue when the last consumer unsubscribes.
	AutoDelete bool
	// Exclusive queues are accessible only by this connection and are
	// deleted when the connection closes.
	Exclusive bool
	Arguments amqp.Table
	// PrefetchCount limits the number of unacknowledged messages
	// delivered to this consumer at a time.
	PrefetchCount int
}

func (c *QueueConfig) defaults() {
	if !c.Durable {
		c.Durable = true
	}
	if c.PrefetchCount == 0 {
		c.PrefetchCount = 1
	}
}

// BindingConfig holds queue-to-exchange binding options.
// Used when both Queue and Exchange are set in Provider.
type BindingConfig struct {
	// RoutingKey is matched against the message routing key by the exchange.
	RoutingKey string
	// NoWait, if true, does not wait for a broker confirmation.
	NoWait    bool
	Arguments amqp.Table
}

// ExchangeConfig holds exchange declaration options.
type ExchangeConfig struct {
	Name string
	// Kind is the exchange type: direct, topic, fanout, or headers.
	Kind string
	// Durable exchanges survive broker restarts.
	Durable bool
	// AutoDelete removes the exchange when the last binding is removed.
	AutoDelete bool
	// Internal exchanges cannot be published to directly by clients.
	Internal bool
	// NoWait, if true, does not wait for a broker confirmation.
	NoWait    bool
	Arguments amqp.Table
}

func (c *ExchangeConfig) defaults() {
	if c.Kind == "" {
		c.Kind = "direct"
	}
	if !c.Durable {
		c.Durable = true
	}
}

// ConsumeConfig holds options passed to amqp.Channel.Consume.
type ConsumeConfig struct {
	// Consumer is the consumer tag used to identify this consumer on the broker.
	// An empty string lets the broker generate a unique tag.
	Consumer string
	// AutoAck, if true, causes the broker to acknowledge messages immediately
	// upon delivery. Use false for manual acknowledgement (recommended).
	AutoAck bool
	// Exclusive, if true, ensures only this consumer receives messages from the queue.
	Exclusive bool
	// NoWait, if true, does not wait for a broker confirmation.
	NoWait    bool
	Arguments amqp.Table
}

// PublishConfig holds options passed to amqp.Channel.Publish.
type PublishConfig struct {
	// RoutingKey is used for direct and topic exchanges.
	// It is ignored by fanout exchanges.
	RoutingKey string
	// Mandatory, if true, returns the message to the publisher if no queue is bound.
	Mandatory bool
	// Immediate, if true, returns the message to the publisher if no consumer is ready.
	Immediate bool
	// Confirm, if true, puts the channel into confirm mode so Publish waits for
	// a broker acknowledgement before returning. Use this when delivery guarantees
	// matter. When false, Publish is fire-and-forget.
	Confirm   bool
	Arguments amqp.Table
}

// Provider holds all configuration for the AMQP service.
// Use Queue for consumer scenarios, Exchange for publisher scenarios,
// or both with Binding for exchange-to-queue routing.
type Provider struct {
	// Name is an optional human-readable identifier for this service instance.
	// It appears in log messages to distinguish multiple services in the same process.
	Name string

	// URLs is a list of AMQP broker addresses.
	// connect() tries each in order, using the first that succeeds.
	URLs   []string
	Config ReconnectConfig

	// Queue holds queue declaration options.
	// Required for Service.Consume.
	Queue *QueueConfig

	// Exchange holds exchange declaration options.
	// Optional for consumers; required for publishers.
	Exchange *ExchangeConfig

	// Binding holds queue-to-exchange binding options.
	// Requires both Queue and Exchange to be set.
	Binding *BindingConfig

	// Consume holds options passed to amqp.Channel.Consume.
	Consume *ConsumeConfig

	// Publish holds options passed to amqp.Channel.PublishWithContext.
	Publish *PublishConfig
}
