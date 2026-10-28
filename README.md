# amqp-wrapper

This module is a wrapper for rabbitmq/amqp091-go package

## Installation

```bash
go get -u github.com/Insly/amqpwrapper/v2
```

## Publishing messages to a queue example

```go
package main

import (
	"context"
	"sync"

	"github.com/Insly/amqpwrapper/v2"
)

func main() {
	ctx, cancelF := context.WithCancel(context.Background())

	wg := new(sync.WaitGroup)

	channel, err := amqpwrapper.NewRabbitChannel(
		ctx,
		wg, // application can wait until all listeners are stopped
		&amqpwrapper.Config{
			URL:          "amqp://user:password@localhost:5672",
			Debug:        true, // true - amqpwrapper writes logs about each delivery
			ConfirmSends: true, // true - the publisher waits for confirmation from RabbitMQ server that a message has been delivered
		},
	)
	if err != nil {
		panic(err)
	}

	go func() {
		<-channel.Done() // closed when the connection is lost and amqpwrapper failed to restore it, or ctx is done
		cancelF()
	}()

	request := map[string]interface{}{"send": "something"}
	if err = channel.Publish(context.Background(), request, "exchange_name", "routing_key"); err != nil {
		panic(err)
	}

	<-ctx.Done()
	wg.Wait()
}
```

## Receiving messages from a queue example

```go
package main

import (
	"context"
	"sync"

	"github.com/Insly/amqpwrapper/v2"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	ctx, cancelF := context.WithCancel(context.Background())

	wg := new(sync.WaitGroup)

	channel, err := amqpwrapper.NewRabbitChannel(
		ctx,
		wg, // application can wait until all listeners are stopped
		&amqpwrapper.Config{
			URL:          "amqp://user:password@localhost:5672",
			Debug:        true, // true - amqpwrapper writes logs about each delivery
			ConfirmSends: true, // true - the publisher waits for confirmation from RabbitMQ server that a message has been delivered
		},
	)
	if err != nil {
		panic(err)
	}

	go func() {
		<-channel.Done() // closed when the connection is lost and amqpwrapper failed to restore it, or ctx is done
		cancelF()
	}()

	if err = channel.DefineExchange(
		"exchange_name",
		false, // false - creates new exchange if it doesn't exist yet; true - relies on such exchange already exists
	); err != nil {
		panic(err)
	}

	if err = channel.SetUpConsumer(
		"exchange_name",
		"routing_key",
		func(ctx context.Context, delivery amqp.Delivery) error {
			// todo: process the delivery
			return nil // nil - the delivery is acknowledged; an error - it's negatively acknowledged without requeue
		},
		amqpwrapper.WithOptionThreads(5), // number of threads which can process deliveries in parallel
	); err != nil {
		panic(err)
	}

	<-ctx.Done()
	wg.Wait()
}
```

## What each affected worker needs

RabbitMQ delivers messages at least once. A message which is not acknowledged when its channel or connection
closes (a reconnect, RabbitMQ node maintenance, a pod restart or crash) is put back to the queue and delivered
again, possibly to another replica. amqpwrapper can't prevent it: the delivery belongs to the closed channel and
can't be acknowledged anymore. A worker whose actions are not idempotent has to recognize a message it has
already handled.

amqpwrapper provides for that:

- `Publish` sets a unique `MessageId`. RabbitMQ keeps it on redeliveries.
- `delivery.Redelivered` is `true` when the message may have been delivered before. Don't rely on it alone:
  the first delivery must leave a record too, so that its redeliveries can find it.
- Both are in the delivery span (`message id`, `redelivered`) and in the `delivery received` debug log.

What to do in such a worker:

1. Upgrade amqpwrapper in the publishers of its messages too. For qmt actions it's mainly BRP. Messages
   published by older versions have no `MessageId`, so the check below is skipped for them.
2. Wrap the listener of each action which is not idempotent with a check of `MessageId` in a store shared by all
   replicas, e.g. Redis. Wrap the whole listener instead of checking inside the action: in qmt workers the
   listener (`processRequest` in `infrastructure/brp/queue.go`) also publishes the result event of the action.
   A skipped copy must not publish it again, otherwise the next action of the flow runs twice as well.

### Example: handling a message once with Redis

```go
package brp

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/Insly/amqpwrapper/v2"
	"github.com/mediocregopher/radix/v3"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
)

const (
	messageProcessing = "processing"
	messageHandled    = "handled"

	processingTTL = 5 * time.Minute // longer than a message can be processed
	handledTTL    = 24 * time.Hour  // longer than a message can wait in a queue
)

// handleOnce makes listener handle each message only once across all replicas of the worker.
// A copy of a handled message is acknowledged without calling listener,
// a copy which comes while the message is still being handled waits for the result.
func handleOnce(redis radix.Client, worker string, listener amqpwrapper.MessageListener) amqpwrapper.MessageListener {
	return func(ctx context.Context, delivery amqp.Delivery) error {
		if delivery.MessageId == "" {
			return listener(ctx, delivery) // published by an older amqpwrapper version, nothing to check by
		}
		// the same message can be routed to queues of several workers, each of them handles it
		key := fmt.Sprintf("%s:handled:%s:%s", worker, delivery.RoutingKey, delivery.MessageId)

		for {
			var ok string
			claim := radix.MaybeNil{Rcv: &ok}
			err := redis.Do(radix.Cmd(&claim, "SET", key, messageProcessing, "NX", "EX", seconds(processingTTL)))
			if err != nil {
				return err
			}
			if !claim.Nil {
				break // claimed: this copy handles the message
			}

			var state string
			get := radix.MaybeNil{Rcv: &state}
			if err = redis.Do(radix.Cmd(&get, "GET", key)); err != nil {
				return err
			}
			if !get.Nil && state == messageHandled {
				logrus.WithField("message_id", delivery.MessageId).Warn("message is already handled, skipped")
				return nil // acknowledges the copy
			}

			// Another copy is being handled by this or another replica. If that replica dies,
			// the claim expires after processingTTL and this copy handles the message.
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(time.Second):
			}
		}

		if err := listener(ctx, delivery); err != nil {
			// release the claim, so a retry of the message is handled again
			if delErr := redis.Do(radix.Cmd(nil, "DEL", key)); delErr != nil {
				logrus.WithError(delErr).WithField("message_id", delivery.MessageId).Error("failed to release the message")
			}
			return err
		}
		if err := redis.Do(radix.Cmd(nil, "SET", key, messageHandled, "EX", seconds(handledTTL))); err != nil {
			logrus.WithError(err).WithField("message_id", delivery.MessageId).Error("failed to mark the message as handled")
		}
		return nil
	}
}

func seconds(d time.Duration) string {
	return strconv.Itoa(int(d.Seconds()))
}
```

Wrap only the listeners of the actions which are not idempotent, e.g. in `infrastructure/brp/queue.go`
(`once` and `redis` are fields to add to the worker):

```go
func (queue *QueueProvider) setUpConsumer(channel *amqpwrapper.RabbitChannel, c consumer) error {
	listener := queue.processRequest(c.action)
	if c.once {
		listener = handleOnce(queue.redis, config.WorkerTag, listener)
	}
	return channel.SetUpConsumer(ActionsExchange, string(c.key), listener)
}
```

Limits:

- If the replica handling a message dies, the claim expires after `processingTTL` and a copy is handled then.
  Whether the dead replica has already done the side effects is unknown, so the action may run twice in this case.
- If the listener fails, the claim is released, and a retry of the message runs the action again.
- `MessageId` identifies one published message. When an upstream action runs twice, it publishes its result
  twice, with different ids. If that matters for an action, build the key from the payload instead, e.g. from the
  request id and the action tag.
