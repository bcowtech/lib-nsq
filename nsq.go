package nsq

import "github.com/nsqio/go-nsq"

func NewConfig() *Config {
	return nsq.NewConfig()
}
