package amqp_connector

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)
import "github.com/streadway/amqp"
import "github.com/Alliera/logging"

var logger = logging.NewDefault("Consumer")

func (conn *Connection) establishConnection(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			connection, err := amqp.Dial(conn.addr)
			if err == nil {
				logger.Debug("Connection successfully established")
				conn.Connection = connection
				return nil
			}
			logger.Error(fmt.Sprintf("Failed to establish connection: %v", err))
			conn.delay()
		}
	}
}

func (conn *Connection) establishChannel(ctx context.Context, channel *Channel) error {
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			ch, err := conn.Connection.Channel()
			if err == nil {
				channel.Channel = ch
				err = ch.Qos(conn.qosPrefetchCount, 0, false)
				if err == nil {
					logger.Debug("Channel successfully established")
					return nil
				}
				logger.Error("Failed to set QoS")
			}
			logger.Error(fmt.Sprintf("Failed to establish channel: %v", err))
			conn.delay()
		}
	}
}

func (conn *Connection) delay() {
	time.Sleep(time.Duration(conn.reconnectionDelaySec) * time.Second)
}

func (ch *Channel) isClosed() bool {
	return atomic.LoadInt32(&ch.closed) == 1
}

func (ch *Channel) isCanceled(consumer string) bool {
	if v, ok := ch.activeConsumers.Load(consumer); !ok || !v.(bool) {
		return true
	}
	return false
}
