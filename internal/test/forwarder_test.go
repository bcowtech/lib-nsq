package test

import (
	"os"
	"strings"
	"testing"

	nsq "github.com/bcowtech/lib-nsq"
)

func TestForwarder(t *testing.T) {
	p, err := nsq.NewForwarder(&nsq.ProducerOption{
		Address: strings.Split(os.Getenv("NSQD_SERVERS"), ","),
		Config:  nsq.NewConfig(),
	})
	if err != nil {
		t.Fatal(err)
	}

	topic := "myTopic"
	for _, word := range []string{"Welcome", "to", "the", "Nsq", "Golang", "client", "library"} {
		p.Write(topic, []byte(word))
	}
	p.Close()
}
