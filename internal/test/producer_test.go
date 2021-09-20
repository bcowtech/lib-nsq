package test

import (
	"os"
	"strings"
	"testing"

	nsq "github.com/bcowtech/lib-nsq"
)

func TestProducer(t *testing.T) {
	p, err := nsq.NewProducer(&nsq.ProducerOption{
		Address:           strings.Split(os.Getenv("NSQD_SERVERS"), ","),
		Config:            nsq.NewConfig(),
		ReplicationFactor: 1,
	})
	if err != nil {
		if p != nil {
			p.Close()
		}
		t.Fatal(err)
	}

	topic := "myTopic"
	for _, word := range []string{"Welcome", "to", "the", "Nsq", "Golang", "client", "library"} {
		p.Write(topic, []byte(word))
	}
	p.Close()
}
