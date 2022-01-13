package amqp_connector

import (
	"context"
	"fmt"
	"github.com/streadway/amqp"
	"sync"
	"sync/atomic"
	"time"
)

type Connection struct {
	*amqp.Connection
	reconnectionDelaySec int
	qosPrefetchCount     int
	addr                 string
}

type Channel struct {
	*amqp.Channel
	activeConsumers sync.Map
	closed          int32
}

func Dial(ctx context.Context, addr string, reconnectionDelaySec int, qosPrefetchCount int) (*Connection, error) {
	connection := &Connection{addr: addr, reconnectionDelaySec: reconnectionDelaySec, qosPrefetchCount: qosPrefetchCount}
	err := connection.establishConnection(ctx)
	if err == nil {
		go func() {
			for {
				reason, ok := <-connection.NotifyClose(make(chan *amqp.Error))
				if !ok {
					break
				}
				logger.Error(fmt.Sprintf("Pool lost connection by reason: %v\n", reason))
				_ = connection.establishConnection(ctx)
			}
		}()
	}
	return connection, err
}

func (conn *Connection) Channel(ctx context.Context) (*Channel, error) {
	channel := &Channel{}
	err := conn.establishChannel(ctx, channel)
	if err == nil {
		go func() {
			for {
				reason, ok := <-channel.NotifyClose(make(chan *amqp.Error))
				if !ok {
					break
				}
				logger.Error(fmt.Sprintf("Pool lost channel by reason: %v\n", reason))
				_ = conn.establishChannel(ctx, channel)
			}
		}()
	}
	return channel, err
}

func (ch *Channel) Consume(ctx context.Context, queue string, consumer string, autoAck bool, exclusive bool, noLocal bool, noWait bool, args amqp.Table) <-chan amqp.Delivery {
	deliveries := make(chan amqp.Delivery)
	ch.activeConsumers.Store(consumer, true)
	go func() {
		for {
			select {
			case <-ctx.Done():
				close(deliveries)
				return
			default:
				d, err := ch.Channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
				if ch.isCanceled(consumer) || ch.isClosed() {
					close(deliveries)
					return
				}
				if err != nil {
					logger.Error(fmt.Sprintf("Failed to consume from '%s' queue: %v", queue, err))
					time.Sleep(4 * time.Second)
					continue
				}
				for msg := range d {
					deliveries <- msg
				}
			}
		}
	}()
	return deliveries
}

func (ch *Channel) CloseChannel() error {
	if ch != nil && ch.Channel != nil {
		if ch.isClosed() {
			return amqp.ErrClosed
		}
		err := ch.Channel.Close()
		if err != nil {
			return err
		}
		atomic.StoreInt32(&ch.closed, 1)
	}
	return nil
}

func (ch *Channel) CancelChannel(consumer string, noWait bool) error {
	if ch != nil && ch.Channel != nil {
		if ch.isCanceled(consumer) {
			return fmt.Errorf("consumer '%s' already canceled", consumer)
		}
		err := ch.Channel.Cancel(consumer, noWait)
		if err != nil {
			return err
		}
		ch.activeConsumers.Store(consumer, false)
	}
	return nil
}
