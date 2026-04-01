package main

import (
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/twg-group/go-rmq"
)

func main() {
	provider := rmq.Provider{
		URLs: []string{"amqp://guest:guest@localhost:5682/"},
		Queue: &rmq.QueueConfig{
			Name: "my-queue-min",
		},
	}

	svc, err := rmq.New(provider)

	if err != nil {
		slog.Error("rmq: service error", "error", err)
		os.Exit(1)
	}

	defer func() {
		if err := svc.Close(); err != nil {
			slog.Error("rmq: close failed", "error", err)
		}
	}()

	svc.Consume(func(msg amqp.Delivery) {
		slog.Info("rmq: received", "body", string(msg.Body))

		if err := msg.Ack(false); err != nil {
			slog.Error("rmq: ack failed", "error", err)
		}
	})

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh
}
