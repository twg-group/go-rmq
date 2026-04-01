package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/twg-group/go-rmq"
)

// Run with debug logging:
//
//	LOG_LEVEL=debug go run ./examples/consumer-full.go
func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	provider := rmq.Provider{
		// URLs is a list of AMQP broker addresses.
		// connect() tries each in order, using the first that succeeds.
		URLs: []string{
			"amqp://guest:guest@localhost:5682/",
			"amqp://guest:guest@localhost:5683/", // fallback node
		},
		Config: rmq.ReconnectConfig{
			// InitialDelay is the first backoff duration before reconnecting.
			InitialDelay: time.Second,
			// MaxDelay is the upper bound for exponential backoff.
			MaxDelay: 30 * time.Second,
			// Multiplier is applied to the current delay on each failed attempt.
			Multiplier: 2.0,
			// Heartbeat is the AMQP heartbeat interval negotiated with the broker.
			Heartbeat: 10 * time.Second,
			// DialTimeout is the maximum time allowed to establish a TCP connection.
			DialTimeout: 5 * time.Second,
		},
		Exchange: &rmq.ExchangeConfig{
			Name: "my-exchange",
			// Kind is the exchange type: direct, topic, fanout, or headers.
			Kind: "direct",
			// Durable exchanges survive broker restarts.
			Durable: true,
			// AutoDelete removes the exchange when the last binding is removed.
			AutoDelete: false,
			// Internal exchanges cannot be published to directly by clients.
			Internal: false,
			// NoWait, if true, does not wait for a broker confirmation.
			NoWait:    false,
			Arguments: amqp.Table{},
		},
		Queue: &rmq.QueueConfig{
			Name: "my-queue",
			// Durable queues survive broker restarts.
			Durable: true,
			// AutoDelete removes the queue when the last consumer unsubscribes.
			AutoDelete: false,
			// Exclusive queues are accessible only by this connection and are
			// deleted when the connection closes.
			Exclusive: false,
			// PrefetchCount limits the number of unacknowledged messages
			// delivered to this consumer at a time.
			PrefetchCount: 10,
			// Arguments are passed to queue declaration.
			// Warning: changing arguments on an existing queue causes PRECONDITION_FAILED.
			Arguments: amqp.Table{
				// x-message-ttl sets the per-message TTL in milliseconds.
				"x-message-ttl": int32(60_000),
				// x-dead-letter-exchange routes rejected messages to a DLX.
				"x-dead-letter-exchange": "my-dlx",
			},
		},
		Binding: &rmq.BindingConfig{
			// RoutingKey is matched against the message routing key by the exchange.
			RoutingKey: "my-routing-key",
			// NoWait, if true, does not wait for a broker confirmation.
			NoWait:    false,
			Arguments: amqp.Table{},
		},
		Consume: &rmq.ConsumeConfig{
			// Consumer is the consumer tag used to identify this consumer on the broker.
			// An empty string lets the broker generate a unique tag.
			Consumer: "my-consumer-tag",
			// AutoAck, if true, causes the broker to acknowledge messages immediately
			// upon delivery. Use false for manual acknowledgement (recommended).
			AutoAck: false,
			// Exclusive, if true, ensures only this consumer receives messages from the queue.
			Exclusive: false,
			// NoWait, if true, does not wait for a broker confirmation.
			NoWait:    false,
			Arguments: amqp.Table{},
		},
	}

	svc, err := rmq.New(
		provider,
		rmq.WithLogger(logger),
		rmq.WithCallbacks(rmq.Callbacks{
			OnConnect: func() {
				logger.Info("rmq: connected")
			},
			OnDisconnect: func() {
				logger.Warn("rmq: disconnected")
			},
			OnError: func(err error) {
				logger.Error("rmq: connection error", "error", err)
			},
		}),
	)

	if err != nil {
		logger.Error("rmq: service error", "error", err)
		os.Exit(1)
	}

	defer func() {
		if err := svc.Close(); err != nil {
			slog.Error("rmq: close failed", "error", err)
		}
	}()

	svc.Consume(func(msg amqp.Delivery) {
		logger.Info("rmq: received", "body", string(msg.Body))

		if err := msg.Ack(false); err != nil {
			logger.Error("rmq: ack failed", "error", err)
		}
	})

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
}
