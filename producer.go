package nsq

import (
	"fmt"
	"sync"
	"time"

	nsq "github.com/nsqio/go-nsq"
)

type Producer struct {
	pool *ProducerPool

	wg       sync.WaitGroup
	mutex    sync.Mutex
	disposed bool
}

func NewProducer(opt *ProducerOption) (*Producer, error) {
	instance := &Producer{}

	var err error
	err = instance.init(opt)
	if err != nil {
		return nil, err
	}
	return instance, nil
}

func (p *Producer) AllHandles() []*nsq.Producer {
	return p.pool.handles
}

func (p *Producer) Handle() *nsq.Producer {
	return p.pool.handles[p.pool.current]
}

func (p *Producer) Write(topic string, body []byte) error {
	if p.disposed {
		return fmt.Errorf("the Producer has been disposed")
	}

	p.wg.Add(1)
	defer p.wg.Done()

	return p.pool.publish(topic, body)
}

func (p *Producer) DeferredWrite(topic string, delay time.Duration, body []byte) error {
	if p.disposed {
		return fmt.Errorf("the Producer has been disposed")
	}

	p.wg.Add(1)
	defer p.wg.Done()

	return p.pool.deferredPublish(topic, delay, body)
}

func (p *Producer) Close() {
	if p.disposed {
		return
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.disposed = true

	p.wg.Wait()
	p.pool.dispose()
}

func (p *Producer) init(opt *ProducerOption) error {
	if opt == nil {
		opt = &ProducerOption{}
	}
	opt.init()

	// config Producer.pool
	{
		var (
			handles []*nsq.Producer
		)
		for _, addr := range opt.Address {
			q, err := nsq.NewProducer(addr, opt.Config)
			if err != nil {
				return err
			}
			if q != nil {
				// test the connection
				err = q.Ping()
				if err != nil {
					return fmt.Errorf("cannot establish connection to '%s'; %v", addr, err)
				}
				handles = append(handles, q)
			}
		}
		assert(len(handles) > 0, "assertion failed: Producer must own at least one nsq producer")

		pool := &ProducerPool{
			handles:           handles,
			replicationFactor: opt.ReplicationFactor,
		}
		pool.init()

		p.pool = pool
	}

	return nil
}
