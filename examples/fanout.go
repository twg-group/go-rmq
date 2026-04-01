package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/twg-group/go-rmq"
)

func main() {
	logLevel := slog.LevelInfo
	if os.Getenv("LOG_LEVEL") == "debug" {
		logLevel = slog.LevelDebug
	}

	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{
		Level: logLevel,
	}))

	// Publisher — declares the fanout exchange and publishes to it.
	// No queue needed on the publisher side.
	publisher, err := rmq.New(
		rmq.Provider{
			Name: "publisher",
			URLs: []string{"amqp://guest:guest@localhost:5682/"},
			Exchange: &rmq.ExchangeConfig{
				Name: "my-fanout",
				Kind: "fanout",
			},
			Publish: &rmq.PublishConfig{
				Confirm: true,
			},
		},
		rmq.WithLogger(logger),
	)
	if err != nil {
		logger.Error("rmq: publisher error", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := publisher.Close(); err != nil {
			logger.Error("rmq: publisher close failed", "error", err)
		}
	}()

	// Consumer — declares queue "a", binds it to the fanout exchange,
	// and starts consuming. No routing key needed for fanout.
	consumer, err := rmq.New(
		rmq.Provider{
			Name: "consumer",
			URLs: []string{"amqp://guest:guest@localhost:5682/"},
			Exchange: &rmq.ExchangeConfig{
				Name: "my-fanout",
				Kind: "fanout",
			},
			Queue: &rmq.QueueConfig{
				Name: "a",
			},
		},
		rmq.WithLogger(logger),
	)
	if err != nil {
		logger.Error("rmq: consumer error", "error", err)
		os.Exit(1)
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			logger.Error("rmq: consumer close failed", "error", err)
		}
	}()

	consumer.Consume(func(msg amqp.Delivery) {
		logger.Info("rmq: received", "queue", "a", "body", string(msg.Body))

		if err := msg.Ack(false); err != nil {
			logger.Error("rmq: ack failed", "error", err)
		}
	})

	// Give the consumer time to set up before publishing.
	time.Sleep(500 * time.Millisecond)

	if err := publisher.Publish(context.Background(), []byte("hello fanout")); err != nil {
		logger.Error("rmq: publish failed", "error", err)
	}

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
}
