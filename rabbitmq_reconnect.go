package rabbitmq

import (
	"sync"
	"time"

	"sync/atomic"

	"github.com/streadway/amqp"
)

const delay = 1 // reconnect after delay seconds

// Connection amqp.Connection wrapper
type Connection struct {
	*amqp.Connection
	closed int32
	sync.RWMutex
}

func (c *Connection) Close() error {
	if c.IsClosed() {
		return amqp.ErrClosed
	}

	atomic.StoreInt32(&c.closed, 1)

	return c.Connection.Close()
}

func (c *Connection) IsClosed() bool {
	return atomic.LoadInt32(&c.closed) == 1
}

// Channel wrap amqp.Connection.Channel, get a auto reconnect channel
func (c *Connection) Channel() (*Channel, error) {
	ch, err := c.Connection.Channel()
	if err != nil {
		return nil, err
	}

	channel := &Channel{
		Channel: ch,
	}

	go func() {
		for {
			_, ok := <-channel.Channel.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			if !ok || channel.IsClosed() {
				_ = channel.Close() // close again, ensure closed flag set when connection closed
				break
			}

			// reconnect if not closed by developer
			for {
				// wait 1s for connection reconnect
				time.Sleep(delay * time.Second)
				c.RLock()
				ch, err := c.Connection.Channel()
				c.RUnlock()
				if err == nil {
					channel.Lock()
					channel.Channel = ch
					channel.Unlock()
					break
				}
			}
		}

	}()

	return channel, nil
}

// Dial wrap amqp.Dial, dial and get a reconnect connection
func Dial(url string) (*Connection, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	connection := &Connection{
		Connection: conn,
	}

	go func() {
		for {
			_, ok := <-connection.Connection.NotifyClose(make(chan *amqp.Error))
			// exit this goroutine if closed by developer
			if !ok {
				break
			}
			// reconnect if not closed by developer
			atomic.StoreInt32(&connection.closed, 1)

			for {
				// wait 1s for reconnect
				time.Sleep(delay * time.Second)
				conn, err := amqp.Dial(url)
				if err == nil {
					connection.Lock()
					connection.Connection = conn
					connection.Unlock()
					atomic.StoreInt32(&connection.closed, 0)
					break
				}
			}
		}
	}()

	return connection, nil
}

// Channel amqp.Channel wrapper
type Channel struct {
	*amqp.Channel
	closed int32
	sync.RWMutex
}

// IsClosed indicate closed by developer
func (ch *Channel) IsClosed() bool {
	return atomic.LoadInt32(&ch.closed) == 1
}

// Close ensure closed flag set
func (ch *Channel) Close() error {
	if ch.IsClosed() {
		return amqp.ErrClosed
	}

	atomic.StoreInt32(&ch.closed, 1)

	return ch.Channel.Close()
}

// Consume warp amqp.Channel.Consume, the returned delivery will end only when channel closed by developer
func (ch *Channel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	deliveries := make(chan amqp.Delivery)

	go func() {
		for {
			d, err := ch.Channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
			if err != nil {
				time.Sleep(delay * time.Second)
				continue
			}

			for msg := range d {
				deliveries <- msg
			}

			// sleep before IsClose call. closed flag may not set before sleep.
			time.Sleep(delay * time.Second)

			if ch.IsClosed() {
				break
			}
		}
	}()

	return deliveries, nil
}

func (ch *Channel) Publish(exchange, routingKey string, mandatory, immediate bool, msg amqp.Publishing) error {
	ch.RLock()
	defer ch.RUnlock()

	if ch.IsClosed() {
		return amqp.ErrClosed
	}

	return ch.Channel.Publish(exchange, routingKey, mandatory, immediate, msg)
}
