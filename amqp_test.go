package amqpwrapper

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
)

type acknowledger struct {
	err error
}

func (a acknowledger) Ack(uint64, bool) error        { return a.err }
func (a acknowledger) Nack(uint64, bool, bool) error { return a.err }
func (a acknowledger) Reject(uint64, bool) error     { return a.err }

func TestRedactURL(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want string
	}{
		{name: "with password", raw: "amqp://user:secret@rabbitmq:5672", want: "amqp://user:xxxxx@rabbitmq:5672"},
		{name: "with vhost", raw: "amqps://user:secret@rabbitmq:5671/qmt", want: "amqps://user:xxxxx@rabbitmq:5671/qmt"},
		{name: "without password", raw: "amqp://user@rabbitmq:5672", want: "amqp://user@rabbitmq:5672"},
		{name: "without credentials", raw: "amqp://rabbitmq:5672", want: "amqp://rabbitmq:5672"},
		{name: "empty", raw: "", want: ""},
		{name: "unparsable", raw: "amqp://user:secret@rabbitmq:port\x7f", want: "<unparsable URL>"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := redactURL(tt.raw); got != tt.want {
				t.Errorf("redactURL(%q) = %q, want %q", tt.raw, got, tt.want)
			}
		})
	}
}

// After a reconnect the old listener's delivery channel is closed while the RabbitChannel holds a new alive
// connection. The listener must stop: reconnect() has already started a new version of it.
func TestListenQueueStopsOnClosedDeliveryChannel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	ch := &RabbitChannel{
		ctx:        ctx,
		cancelFunc: cancel,
		waitGroup:  new(sync.WaitGroup),
		conn:       new(amqp.Connection), // not closed
		channel:    new(amqp.Channel),
	}

	msgChannel := make(chan amqp.Delivery)
	close(msgChannel)

	stopped := make(chan struct{})
	go func() {
		ch.listenQueue("test", 1, msgChannel, nil, newSemaphore(defaultThreadsCount))
		close(stopped)
	}()

	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("listener is not stopped after its delivery channel is closed")
	}

	if ctx.Err() != nil {
		t.Error("listener must not cancel the channel when its delivery channel is closed")
	}
	ch.waitGroup.Wait()
}

func TestProcessDeliveryNackFailure(t *testing.T) {
	tests := []struct {
		name       string
		nackErr    error
		wantCancel bool
	}{
		// e.g. the delivery of a listener whose channel is closed by a reconnect
		{name: "channel is closed", nackErr: amqp.ErrClosed, wantCancel: false},
		{name: "other error", nackErr: errors.New("broken pipe"), wantCancel: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			ch := &RabbitChannel{ctx: ctx, cancelFunc: cancel, tracer: otel.Tracer("test")}
			threads := newSemaphore(1)
			threads.Acquire(ctx)

			delivery := amqp.Delivery{Acknowledger: acknowledger{err: tt.nackErr}}
			callback := func(context.Context, amqp.Delivery) error { return errors.New("action failed") }
			ch.processDelivery(delivery, "test", 1, callback, threads)

			if cancelled := ctx.Err() != nil; cancelled != tt.wantCancel {
				t.Errorf("channel cancelled = %v, want %v", cancelled, tt.wantCancel)
			}
			if len(threads.ch) != 0 {
				t.Error("thread is not released")
			}
		})
	}
}

type logLevels []logrus.Level

func (l *logLevels) Levels() []logrus.Level { return logrus.AllLevels }

func (l *logLevels) Fire(entry *logrus.Entry) error {
	*l = append(*l, entry.Level)
	return nil
}

func TestProcessDeliveryAckFailure(t *testing.T) {
	tests := []struct {
		name      string
		ackErr    error
		wantLevel logrus.Level
	}{
		// e.g. the delivery of a listener whose channel is closed by a reconnect
		{name: "channel is closed", ackErr: amqp.ErrClosed, wantLevel: logrus.WarnLevel},
		{name: "other error", ackErr: errors.New("broken pipe"), wantLevel: logrus.ErrorLevel},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logged := new(logLevels)
			hooks := logrus.StandardLogger().ReplaceHooks(logrus.LevelHooks{})
			defer logrus.StandardLogger().ReplaceHooks(hooks)
			logrus.AddHook(logged)

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			ch := &RabbitChannel{ctx: ctx, cancelFunc: cancel, tracer: otel.Tracer("test")}
			delivery := amqp.Delivery{Acknowledger: acknowledger{err: tt.ackErr}}
			callback := func(context.Context, amqp.Delivery) error { return nil }
			ch.processDelivery(delivery, "test", 1, callback, newSemaphore(1))

			if len(*logged) != 1 || (*logged)[0] != tt.wantLevel {
				t.Errorf("logged levels = %v, want [%v]", *logged, tt.wantLevel)
			}
			if ctx.Err() != nil {
				t.Error("failed ack must not cancel the channel")
			}
		})
	}
}
