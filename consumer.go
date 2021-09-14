package nsq

import (
	"fmt"
	"sync"

	"github.com/nsqio/go-nsq"
)

type Consumer struct {
	NsqAddress              string // nsqd:127.0.0.1:4150,127.0.0.2:4150 -or- nsqlookupd:127.0.0.1:4161,127.0.0.2:4161
	Channel                 string
	HandlerConcurrency      int
	Config                  *Config
	MessageHandler          MessageHandleProc
	UnhandledMessageHandler MessageHandleProc

	consumers []*nsq.Consumer
	wg        sync.WaitGroup

	mutex       sync.Mutex
	initialized bool
	running     bool
	disposed    bool
}

func (c *Consumer) Subscribe(topics []string) error {
	if c.disposed {
		logger.Panic("the Consumer has been disposed")
	}
	if c.running {
		logger.Panic("the Consumer is running")
	}

	var err error
	c.mutex.Lock()
	defer func() {
		if err != nil {
			c.running = false
			c.disposed = true
		}
		c.mutex.Unlock()
	}()
	c.init()
	c.running = true

	for _, topic := range topics {
		var consumer *nsq.Consumer
		consumer, err = nsq.NewConsumer(topic, c.Channel, c.Config)
		if err != nil {
			return err
		}

		handler := c.createMessageHandler(topic)

		// bind the MessageHandler
		consumer.AddConcurrentHandlers(
			handler,
			c.HandlerConcurrency)

		err = c.connectToNsq(consumer)
		if err != nil {
			return err
		}

		c.consumers = append(c.consumers, consumer)
	}
	return nil
}

func (c *Consumer) Close() {
	if c.disposed {
		return
	}

	c.mutex.Lock()
	defer func() {
		c.running = false
		c.disposed = true
		// dispose
		c.consumers = nil
		c.mutex.Unlock()
	}()

	c.wg.Wait()

	for _, consumer := range c.consumers {
		consumer.Stop()
	}
}

func (c *Consumer) connectToNsq(consumer *nsq.Consumer) error {
	service, addrs := SplitConnectionString(c.NsqAddress)
	if len(service) == 0 {
		return fmt.Errorf("no service offered")
	}
	if len(addrs) == 0 {
		return fmt.Errorf("no address offered")
	}

	switch service {
	case SERVICE_NSQD:
		return consumer.ConnectToNSQDs(addrs)
	case SERVICE_NSQLOOKUPD:
		return consumer.ConnectToNSQLookupds(addrs)
	}
	return fmt.Errorf("cannot connecto to nsq; unsupported service '%s'", service)
}

func (c *Consumer) init() {
	if c.initialized {
		return
	}

	if c.Config == nil {
		c.Config = nsq.NewConfig()
	}

	c.initialized = true
}

func (c *Consumer) createMessageHandler(topic string) nsq.HandlerFunc {
	var proc = func(m *nsq.Message) error {
		c.wg.Add(1)
		defer c.wg.Done()

		message := &Message{
			Message: m,
			Topic:   topic,
		}

		if c.MessageHandler != nil {
			return c.MessageHandler(message)
		}
		if c.UnhandledMessageHandler != nil {
			return c.UnhandledMessageHandler(message)
		}
		return nil
	}

	// type assertion
	var _ nsq.HandlerFunc = proc
	return proc
}
