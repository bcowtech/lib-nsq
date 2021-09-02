package nsq

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	nsq "github.com/nsqio/go-nsq"
)

type Producer struct {
	handles []*nsq.Producer

	current  int32
	wg       sync.WaitGroup
	mutex    sync.Mutex
	disposed bool
}

func NewProducer(opt *ProducerOption) (*Producer, error) {
	instance := &Producer{
		current: -1,
	}

	var err error
	err = instance.init(opt)
	if err != nil {
		return nil, err
	}
	return instance, nil
}

func (p *Producer) AllHandles() []*nsq.Producer {
	return p.handles
}

func (p *Producer) Handle() *nsq.Producer {
	return p.nextNsqProducer()
}

func (p *Producer) Write(topic string, body []byte) error {
	if p.disposed {
		return fmt.Errorf("the Producer has been disposed")
	}

	p.wg.Add(1)
	defer p.wg.Done()

	var (
		retry = len(p.handles)

		err    error
		handle *nsq.Producer
	)

	for attempts := 0; attempts < retry; attempts++ {
		handle = p.nextNsqProducer()

		err = handle.Publish(topic, body)
		if err != nil {
			continue
		}
		break
	}
	return err
}

func (p *Producer) DeferredWrite(topic string, delay time.Duration, body []byte) error {
	if p.disposed {
		return fmt.Errorf("the Producer has been disposed")
	}

	p.wg.Add(1)
	defer p.wg.Done()

	var (
		retry = len(p.handles)

		err    error
		handle *nsq.Producer
	)

	for attempts := 0; attempts < retry; attempts++ {
		handle = p.nextNsqProducer()

		err = handle.DeferredPublish(topic, delay, body)
		if err != nil {
			continue
		}
		break
	}
	return err
}

func (p *Producer) Close() {
	if p.disposed {
		return
	}

	p.mutex.Lock()
	defer p.mutex.Unlock()

	p.disposed = true

	p.wg.Wait()
	// stop all nsq producers
	for _, handle := range p.handles {
		handle.Stop()
	}
}

func (p *Producer) nextNsqProducer() *nsq.Producer {
	var (
		ubound int32 = int32(len(p.handles)) - 1
	)
	if ubound == 0 {
		return p.handles[0]
	}

	// set p.current to 0, if it reach ubound
	if atomic.CompareAndSwapInt32(&p.current, ubound, 0) {
		return p.handles[0]
	}
	next := atomic.AddInt32(&p.current, 1)
	return p.handles[next]
}

func (p *Producer) init(opt *ProducerOption) error {
	if opt == nil {
		opt = &ProducerOption{}
	}
	if len(opt.Address) == 0 {
		opt.Address = []string{
			"localhost:4150",
		}
	}

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
			p.handles = append(p.handles, q)
		}
	}

	assert(len(p.handles) > 0, "assertion failed: Producer must own at least one nsq producer")
	return nil
}
