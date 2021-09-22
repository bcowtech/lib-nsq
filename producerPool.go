package nsq

import (
	"fmt"
	"sync/atomic"
	"time"

	"github.com/nsqio/go-nsq"
)

type ProducerPool struct {
	handles []*nsq.Producer

	replicationFactor int32

	current     int32
	disposed    bool
	initialized bool
}

func (p *ProducerPool) publish(topic string, body []byte) error {
	if p.disposed {
		return fmt.Errorf("the ProducerPool has been disposed")
	}
	if !p.initialized {
		logger.Panic("the ProducerPool haven't be initialized yet")
	}

	var (
		size     = len(p.handles)
		attempts = size

		err  error
		next int32
	)

	for attempt := 0; attempt < attempts; attempt++ {
		next = p.next()

		handle := p.handles[next]
		err = handle.Publish(topic, body)
		if err != nil {
			continue
		}
		break
	}
	if err != nil {
		return err
	}

	if size > 1 && p.replicationFactor > 1 {
		var (
			sent int32 = 0
		)
		for offset := 1; offset < attempts; offset++ {
			index := next + int32(offset)
			if index >= int32(size) {
				index = index - int32(size)
			}

			handle := p.handles[index]
			err = handle.Publish(topic, body)
			if err != nil {
				continue
			}
			sent++

			if sent == p.replicationFactor {
				break
			}
		}
	}
	return err
}

func (p *ProducerPool) deferredPublish(topic string, delay time.Duration, body []byte) error {
	if p.disposed {
		return fmt.Errorf("the ProducerPool has been disposed")
	}
	if !p.initialized {
		logger.Panic("the ProducerPool haven't be initialized yet")
	}

	var (
		size     = len(p.handles)
		attempts = size

		err  error
		next int32
	)

	for attempt := 0; attempt < attempts; attempt++ {
		next = p.next()

		handle := p.handles[next]
		err = handle.DeferredPublish(topic, delay, body)
		if err != nil {
			continue
		}
		break
	}
	if err != nil {
		return err
	}

	if size > 1 && p.replicationFactor > 1 {
		var (
			sent int32 = 0
		)
		for offset := 1; offset < attempts; offset++ {
			index := next + int32(offset)
			if index >= int32(size) {
				index = index - int32(size)
			}

			handle := p.handles[index]
			err = handle.DeferredPublish(topic, delay, body)
			if err != nil {
				continue
			}
			sent++

			if sent == p.replicationFactor {
				break
			}
		}
	}
	return err
}

func (p *ProducerPool) dispose() {
	if p.disposed {
		return
	}

	p.disposed = true

	// stop all nsq producers
	for _, handle := range p.handles {
		handle.Stop()
	}
}

func (p *ProducerPool) init() {
	if p.initialized {
		return
	}

	p.current = -1

	var size = int32(len(p.handles))
	if p.replicationFactor > size {
		p.replicationFactor = size
	}

	p.initialized = true
}

func (p *ProducerPool) next() int32 {
	// TODO: use round-robin strategy

	var (
		ubound int32 = int32(len(p.handles)) - 1
	)
	if ubound == 0 {
		return 0
	}

	// set p.current to 0, if it reach ubound
	if atomic.CompareAndSwapInt32(&p.current, ubound, 0) {
		return 0
	}
	next := atomic.AddInt32(&p.current, 1)
	return next
}
