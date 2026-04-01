package rmq

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

// Callbacks holds optional lifecycle hooks invoked by Service on connection
// state transitions. Any field left nil is replaced with a default that logs
// the event via the service logger.
type Callbacks struct {
	// OnConnect is called after each successful connection or reconnection.
	OnConnect func()
	// OnDisconnect is called when an established connection is lost unexpectedly.
	OnDisconnect func()
	// OnError is called when a connection attempt fails.
	OnError func(err error)
}

// Option is a functional option for configuring a Service.
type Option func(*Service)

// WithLogger sets a custom structured logger on the Service.
// When not provided, slog.Default() is used.
func WithLogger(logger *slog.Logger) Option {
	return func(s *Service) {
		s.logger = logger
	}
}

// WithCallbacks sets lifecycle callbacks on the Service.
// Individual nil fields are replaced with defaults that log the event.
func WithCallbacks(cb Callbacks) Option {
	return func(s *Service) {
		s.callbacks = cb
	}
}

// dialFunc is the signature for establishing an AMQP connection.
// Replaced in tests to inject a mock dialer.
type dialFunc func(url string, cfg amqp.Config) (*amqp.Connection, error)

// Service manages a single AMQP connection with automatic reconnection.
// Use New to construct a Service; the zero value is not valid.
//
// All public methods are safe for concurrent use.
type Service struct {
	provider  Provider
	logger    *slog.Logger
	callbacks Callbacks
	dial      dialFunc

	mu       sync.RWMutex
	amqpConn *amqp.Connection

	// ctx and cancel are initialised in New and cancelled in Close.
	ctx    context.Context
	cancel context.CancelFunc

	// connCh broadcasts a ready connection to all goroutines that need one.
	// reconnectLoop sends on connCh each time a new connection is established.
	// The channel is buffered so a send never blocks when no one is listening yet.
	// Receivers must re-send to connCh so other waiters also get the signal.
	connCh chan *amqp.Connection

	// stopCh is closed exactly once by Close to signal all goroutines to exit.
	stopCh    chan struct{}
	closeOnce sync.Once

	// goroutineWg tracks background goroutines (reconnectLoop, Consume) so
	// Close can wait for them to exit before closing the AMQP connection.
	goroutineWg sync.WaitGroup

	// wg counts in-flight message handlers so Close can drain them before
	// closing the connection.
	wg sync.WaitGroup

	// publishCh is the AMQP channel used by Publish, opened lazily and
	// reopened after reconnects. publishMu guards it against concurrent access.
	publishCh *amqp.Channel
	publishMu sync.Mutex
}

// New creates a Service with the given provider configuration and applies any
// supplied options. The initial connection attempt is made synchronously;
// if it fails, reconnection continues in the background using exponential
// backoff as defined by provider.Config.
//
// Callers must call Close when the service is no longer needed.
//
//	svc, err := rmq.New(provider)
//	svc, err := rmq.New(provider, rmq.WithLogger(logger))
//	svc, err := rmq.New(provider, rmq.WithLogger(logger), rmq.WithCallbacks(cb))
func New(provider Provider, opts ...Option) (*Service, error) {
	provider.Config.defaults()

	if provider.Queue != nil {
		provider.Queue.defaults()
	}
	if provider.Exchange != nil {
		provider.Exchange.defaults()
	}

	svc := &Service{
		provider: provider,
		connCh:   make(chan *amqp.Connection, 1),
		stopCh:   make(chan struct{}),
		dial:     amqp.DialConfig,
	}

	for _, opt := range opts {
		opt(svc)
	}

	if svc.logger == nil {
		svc.logger = slog.Default()
	}
	if svc.provider.Name != "" {
		svc.logger = svc.logger.With("service", svc.provider.Name)
	}
	if svc.callbacks.OnConnect == nil {
		svc.callbacks.OnConnect = func() {
			svc.logger.Info("rmq: connected")
		}
	}
	if svc.callbacks.OnDisconnect == nil {
		svc.callbacks.OnDisconnect = func() {
			svc.logger.Warn("rmq: disconnected")
		}
	}
	if svc.callbacks.OnError == nil {
		svc.callbacks.OnError = func(err error) {
			svc.logger.Error("rmq: connection error", "error", err)
		}
	}

	if err := svc.init(); err != nil {
		return nil, err
	}

	return svc, nil
}

// init creates the service context and starts the reconnect loop.
func (s *Service) init() error {
	s.logger.Debug("rmq: initializing")
	s.ctx, s.cancel = context.WithCancel(context.Background())

	s.goroutineWg.Add(1)
	go func() {
		defer s.goroutineWg.Done()
		s.reconnectLoop()
	}()

	s.logger.Debug("rmq: initialized")
	return nil
}

// Close shuts the service down in three ordered steps:
//  1. Cancels the context and waits for all background goroutines to exit.
//  2. Waits for any in-flight message handlers to finish.
//  3. Closes the AMQP connection (implicitly closes all derived channels).
//
// It is safe to call Close more than once; subsequent calls are no-ops.
func (s *Service) Close() error {
	s.logger.Info("rmq: closing")

	// Step 1: stop all background goroutines.
	s.cancel()
	s.closeOnce.Do(func() { close(s.stopCh) })
	s.goroutineWg.Wait()

	// Step 2: wait for in-flight message handlers.
	s.wg.Wait()

	// Step 3: close the connection; all derived channels close with it.
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.amqpConn != nil && !s.amqpConn.IsClosed() {
		if err := s.amqpConn.Close(); err != nil {
			s.logger.Error("rmq: close failed", "error", err)
			return fmt.Errorf("rmq: close: %w", err)
		}
	}

	s.publishMu.Lock()
	s.publishCh = nil
	s.publishMu.Unlock()

	s.logger.Info("rmq: closed")
	return nil
}

// waitConn blocks until a live connection is available or the service is
// stopping. On success it re-sends the connection to connCh so other waiters
// also receive it, and returns the connection.
func (s *Service) waitConn() (*amqp.Connection, error) {
	select {
	case conn := <-s.connCh:
		// Re-send so the next waiter (or this goroutine on reconnect) can also receive.
		s.connCh <- conn
		return conn, nil
	case <-s.stopCh:
		return nil, fmt.Errorf("rmq: service is closing")
	case <-s.ctx.Done():
		return nil, fmt.Errorf("rmq: context cancelled")
	}
}

// Channel opens and returns a new AMQP channel on the live connection.
// It blocks until a connection is available or the service is closing.
// The caller is responsible for closing the returned channel.
func (s *Service) Channel() (*amqp.Channel, error) {
	conn, err := s.waitConn()
	if err != nil {
		return nil, err
	}
	return s.openChannelOn(conn)
}

// openChannelOn opens an AMQP channel on the given connection.
func (s *Service) openChannelOn(conn *amqp.Connection) (*amqp.Channel, error) {
	if conn == nil || conn.IsClosed() {
		return nil, fmt.Errorf("rmq: connection not available")
	}
	ch, err := conn.Channel()
	if err != nil {
		return nil, fmt.Errorf("rmq: open channel: %w", err)
	}
	return ch, nil
}

// CloseChannel closes ch if it is non-nil and not already closed.
// Errors are logged and suppressed; callers do not need to check them.
func (s *Service) CloseChannel(ch *amqp.Channel) {
	if ch != nil && !ch.IsClosed() {
		if err := ch.Close(); err != nil {
			s.logger.Error("rmq: channel close failed", "error", err)
		}
	}
}

// Consume starts a long-running goroutine that delivers messages from the
// configured queue to handler. The goroutine survives reconnections: on each
// new connection it re-declares the full topology (exchange → queue → binding
// → QoS) before resuming delivery.
//
// Panics inside handler are recovered; the offending message is nacked with
// requeue=true so it is not lost.
//
// Consume requires provider.Queue to be set; it logs an error and returns
// immediately if it is not.
func (s *Service) Consume(handler func(amqp.Delivery)) {
	if s.provider.Queue == nil {
		s.logger.Error("rmq: consume requires queue config in provider")
		return
	}

	s.goroutineWg.Add(1)
	go func() {
		defer s.goroutineWg.Done()

		for {
			conn, err := s.waitConn()
			if err != nil {
				// Service is stopping.
				s.logger.Info("rmq: consume stopped", "reason", err)
				return
			}

			ch, msgs, err := s.setupConsumer(conn)
			if err != nil {
				// Connection may have died between waitConn and setupConsumer.
				// Log and loop back to wait for the next connection.
				s.logger.Debug("rmq: consumer setup failed, waiting for reconnect", "error", err)
				continue
			}

			s.logger.Info("rmq: consumer started", "queue", s.provider.Queue.Name)
			s.consumeLoop(ch, msgs, handler)
			// consumeLoop returned — connection lost, loop back to waitConn.
		}
	}()
}

// setupConsumer declares the exchange, queue, and binding on conn, applies
// QoS, and registers the consumer.
func (s *Service) setupConsumer(conn *amqp.Connection) (*amqp.Channel, <-chan amqp.Delivery, error) {
	ch, err := s.openChannelOn(conn)
	if err != nil {
		return nil, nil, fmt.Errorf("open channel: %w", err)
	}

	if exc := s.provider.Exchange; exc != nil {
		if err := ch.ExchangeDeclare(
			exc.Name,
			exc.Kind,
			exc.Durable,
			exc.AutoDelete,
			exc.Internal,
			exc.NoWait,
			exc.Arguments,
		); err != nil {
			s.CloseChannel(ch)
			return nil, nil, fmt.Errorf("declare exchange %q: %w", exc.Name, err)
		}
		s.logger.Debug("rmq: exchange declared", "exchange", exc.Name)
	}

	if q := s.provider.Queue; q != nil {
		if _, err := ch.QueueDeclare(
			q.Name,
			q.Durable,
			q.AutoDelete,
			q.Exclusive,
			false,
			q.Arguments,
		); err != nil {
			s.CloseChannel(ch)
			return nil, nil, fmt.Errorf("declare queue %q: %w", q.Name, err)
		}
		s.logger.Debug("rmq: queue declared", "queue", q.Name)

		if s.provider.Exchange != nil {
			b := s.provider.Binding
			var routingKey string
			var noWait bool
			var arguments amqp.Table
			if b != nil {
				routingKey = b.RoutingKey
				noWait = b.NoWait
				arguments = b.Arguments
			}
			if err := ch.QueueBind(
				q.Name,
				routingKey,
				s.provider.Exchange.Name,
				noWait,
				arguments,
			); err != nil {
				s.CloseChannel(ch)
				return nil, nil, fmt.Errorf("bind queue %q to exchange %q: %w", q.Name, s.provider.Exchange.Name, err)
			}
			s.logger.Debug("rmq: queue bound",
				"queue", q.Name,
				"exchange", s.provider.Exchange.Name,
				"routing_key", routingKey,
			)
		}

		if err := ch.Qos(q.PrefetchCount, 0, false); err != nil {
			s.CloseChannel(ch)
			return nil, nil, fmt.Errorf("set qos: %w", err)
		}
	}

	cfg := s.consumeConfig()
	msgs, err := ch.Consume(
		s.provider.Queue.Name,
		cfg.Consumer,
		cfg.AutoAck,
		cfg.Exclusive,
		false, // noLocal is not supported by RabbitMQ
		cfg.NoWait,
		cfg.Arguments,
	)
	if err != nil {
		s.CloseChannel(ch)
		return nil, nil, fmt.Errorf("start consume on queue %q: %w", s.provider.Queue.Name, err)
	}

	return ch, msgs, nil
}

// consumeLoop reads from msgs and dispatches each delivery to handleMessage
// until the context is cancelled, the stop signal is received, or the broker
// closes the delivery channel.
func (s *Service) consumeLoop(ch *amqp.Channel, msgs <-chan amqp.Delivery, handler func(amqp.Delivery)) {
	defer s.CloseChannel(ch)

	for {
		select {
		case <-s.ctx.Done():
			s.logger.Info("rmq: consume loop stopped (context cancelled)")
			return
		case <-s.stopCh:
			s.logger.Info("rmq: consume loop stopped (service closing)")
			return
		case msg, ok := <-msgs:
			if !ok {
				s.logger.Warn("rmq: delivery channel closed, will reconnect")
				return
			}
			s.handleMessage(msg, handler)
		}
	}
}

// handleMessage invokes handler inside a deferred recover so that a panic
// does not crash the consumer goroutine. On panic the message is nacked with
// requeue=true. The WaitGroup is incremented so Close drains handlers before
// closing the connection.
func (s *Service) handleMessage(msg amqp.Delivery, handler func(amqp.Delivery)) {
	s.wg.Add(1)
	defer s.wg.Done()

	defer func() {
		if r := recover(); r != nil {
			s.logger.Error("rmq: handler panic", "recover", r)
			if err := msg.Nack(false, true); err != nil {
				s.logger.Error("rmq: nack after panic failed", "error", err)
			}
		}
	}()

	handler(msg)
}

// consumeConfig returns the ConsumeConfig from the provider, or a safe
// default (manual ack) when none is set.
func (s *Service) consumeConfig() ConsumeConfig {
	if s.provider.Consume != nil {
		return *s.provider.Consume
	}
	return ConsumeConfig{AutoAck: false}
}

// Publish sends body to the configured exchange or queue.
// The channel is opened lazily on the first call and reused across subsequent
// calls. If provider.Publish.Confirm is true, Publish waits for a broker
// acknowledgement before returning; otherwise it is fire-and-forget.
func (s *Service) Publish(ctx context.Context, body []byte) error {
	cfg := s.publishConfig()

	ch, err := s.getPublishChannel(cfg.Confirm)
	if err != nil {
		return fmt.Errorf("rmq: publish: %w", err)
	}

	exchange, routingKey := s.publishTarget()

	if cfg.Confirm {
		confirm, err := ch.PublishWithDeferredConfirmWithContext(
			ctx,
			exchange,
			routingKey,
			cfg.Mandatory,
			cfg.Immediate,
			amqp.Publishing{
				DeliveryMode: amqp.Persistent,
				ContentType:  "application/octet-stream",
				Body:         body,
			},
		)
		if err != nil {
			return fmt.Errorf("rmq: publish: %w", err)
		}
		ok, err := confirm.WaitContext(ctx)
		if err != nil {
			return fmt.Errorf("rmq: confirm wait: %w", err)
		}
		if !ok {
			return fmt.Errorf("rmq: message nacked by broker")
		}
		s.logger.Debug("rmq: publish confirmed by broker")
		return nil
	}

	return ch.PublishWithContext(
		ctx,
		exchange,
		routingKey,
		cfg.Mandatory,
		cfg.Immediate,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/octet-stream",
			Body:         body,
		},
	)
}

// getPublishChannel returns the cached publish channel, opening it if needed.
// publishMu is released before blocking on waitConn to avoid a deadlock with
// the reconnect path which also acquires publishMu.
func (s *Service) getPublishChannel(confirm bool) (*amqp.Channel, error) {
	s.publishMu.Lock()
	if s.publishCh != nil && !s.publishCh.IsClosed() {
		defer s.publishMu.Unlock()
		return s.publishCh, nil
	}
	s.publishMu.Unlock()

	conn, err := s.waitConn()
	if err != nil {
		return nil, err
	}

	// Re-acquire and check again — another goroutine may have opened the channel.
	s.publishMu.Lock()
	defer s.publishMu.Unlock()

	if s.publishCh != nil && !s.publishCh.IsClosed() {
		return s.publishCh, nil
	}

	ch, err := s.setupPublisher(conn, confirm)
	if err != nil {
		return nil, err
	}

	s.publishCh = ch
	return ch, nil
}

// setupPublisher opens a channel on conn, declares the full topology
// (exchange → queue → binding), and optionally enables confirm mode.
func (s *Service) setupPublisher(conn *amqp.Connection, confirm bool) (*amqp.Channel, error) {
	ch, err := s.openChannelOn(conn)
	if err != nil {
		return nil, fmt.Errorf("open channel: %w", err)
	}

	if exc := s.provider.Exchange; exc != nil {
		if err := ch.ExchangeDeclare(
			exc.Name,
			exc.Kind,
			exc.Durable,
			exc.AutoDelete,
			exc.Internal,
			exc.NoWait,
			exc.Arguments,
		); err != nil {
			s.CloseChannel(ch)
			return nil, fmt.Errorf("declare exchange %q: %w", exc.Name, err)
		}
		s.logger.Debug("rmq: publisher exchange declared", "exchange", exc.Name)
	}

	if q := s.provider.Queue; q != nil {
		if _, err := ch.QueueDeclare(
			q.Name,
			q.Durable,
			q.AutoDelete,
			q.Exclusive,
			false,
			q.Arguments,
		); err != nil {
			s.CloseChannel(ch)
			return nil, fmt.Errorf("declare queue %q: %w", q.Name, err)
		}
		s.logger.Debug("rmq: publisher queue declared", "queue", q.Name)

		if s.provider.Exchange != nil {
			b := s.provider.Binding
			var routingKey string
			var noWait bool
			var arguments amqp.Table
			if b != nil {
				routingKey = b.RoutingKey
				noWait = b.NoWait
				arguments = b.Arguments
			}
			if err := ch.QueueBind(
				q.Name,
				routingKey,
				s.provider.Exchange.Name,
				noWait,
				arguments,
			); err != nil {
				s.CloseChannel(ch)
				return nil, fmt.Errorf("bind queue %q to exchange %q: %w", q.Name, s.provider.Exchange.Name, err)
			}
			s.logger.Debug("rmq: publisher queue bound",
				"queue", q.Name,
				"exchange", s.provider.Exchange.Name,
				"routing_key", routingKey,
			)
		}
	}

	if confirm {
		if err := ch.Confirm(false); err != nil {
			s.CloseChannel(ch)
			return nil, fmt.Errorf("enable confirm mode: %w", err)
		}
		s.logger.Debug("rmq: publisher confirm mode enabled")
	}

	return ch, nil
}

// publishTarget returns the exchange and routing key to publish to.
// When no exchange is configured, publishes directly to the queue via the
// default exchange.
func (s *Service) publishTarget() (exchange, routingKey string) {
	if exc := s.provider.Exchange; exc != nil {
		exchange = exc.Name
		if s.provider.Publish != nil {
			routingKey = s.provider.Publish.RoutingKey
		}
		return
	}
	if q := s.provider.Queue; q != nil {
		routingKey = q.Name
	}
	return
}

// publishConfig returns PublishConfig from the provider or safe defaults.
func (s *Service) publishConfig() PublishConfig {
	if s.provider.Publish != nil {
		return *s.provider.Publish
	}
	return PublishConfig{}
}

// reconnectLoop is the single goroutine responsible for establishing and
// monitoring the AMQP connection. On each successful connect it sends the
// connection to connCh so all waiters (Consume, Publish, Channel) unblock.
// It retries with exponential backoff on failure.
func (s *Service) reconnectLoop() {
	bo := &backoff{
		current:    s.provider.Config.InitialDelay,
		initial:    s.provider.Config.InitialDelay,
		max:        s.provider.Config.MaxDelay,
		multiplier: s.provider.Config.Multiplier,
	}

	for {
		conn, closeCh, err := s.connect()
		if err != nil {
			s.logger.Warn("rmq: connect failed", "error", err, "next_delay", bo.current)

			select {
			case <-s.ctx.Done():
				return
			case <-s.stopCh:
				return
			case <-time.After(bo.next()):
				continue
			}
		}

		bo.reset()

		// Drain stale connection from connCh if present, then publish fresh one.
		select {
		case <-s.connCh:
		default:
		}
		s.connCh <- conn

		// Wait for the connection to close or the service to stop.
		select {
		case err, ok := <-closeCh:
			if !ok || err == nil {
				s.logger.Info("rmq: graceful close, reconnect loop stopped")
				return
			}
			s.callbacks.OnDisconnect()
			s.logger.Warn("rmq: connection lost", "error", err)

			// Invalidate publishCh so it is re-opened on the next connection.
			s.publishMu.Lock()
			s.publishCh = nil
			s.publishMu.Unlock()

			// Drain connCh so waitConn blocks until the next connection is ready.
			select {
			case <-s.connCh:
			default:
			}

		case <-s.ctx.Done():
			s.logger.Info("rmq: context cancelled, reconnect loop stopped")
			return
		case <-s.stopCh:
			s.logger.Info("rmq: stop signal received, reconnect loop stopped")
			return
		}
	}
}

// connect dials each URL in order and returns the first successful connection
// along with its NotifyClose channel. Subscribing to NotifyClose here, before
// the connection is shared, ensures no close event can be missed.
func (s *Service) connect() (*amqp.Connection, <-chan *amqp.Error, error) {
	var lastErr error

	for _, url := range s.provider.URLs {
		conn, err := s.dial(url, amqp.Config{
			Heartbeat: s.provider.Config.Heartbeat,
			Dial:      amqp.DefaultDial(s.provider.Config.DialTimeout),
		})
		if err != nil {
			lastErr = err
			s.callbacks.OnError(err)
			continue
		}

		closeCh := conn.NotifyClose(make(chan *amqp.Error, 1))

		s.mu.Lock()
		s.amqpConn = conn
		s.mu.Unlock()

		s.callbacks.OnConnect()
		return conn, closeCh, nil
	}

	return nil, nil, fmt.Errorf("rmq: all urls failed: %w", lastErr)
}

// IsConnected reports whether the underlying AMQP connection is currently open.
func (s *Service) IsConnected() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.amqpConn != nil && !s.amqpConn.IsClosed()
}

// backoff implements truncated exponential backoff.
type backoff struct {
	current    time.Duration
	initial    time.Duration
	max        time.Duration
	multiplier float64
}

// next returns the current delay and advances to the next value, capped at max.
func (b *backoff) next() time.Duration {
	d := b.current
	b.current = time.Duration(float64(b.current) * b.multiplier)
	if b.current > b.max {
		b.current = b.max
	}
	return d
}

// reset restores the delay to its initial value.
func (b *backoff) reset() {
	b.current = b.initial
}
