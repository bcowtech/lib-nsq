package nsq

import "github.com/nsqio/go-nsq"

type Message struct {
	*nsq.Message

	Topic string
}
