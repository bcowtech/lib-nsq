package test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	nsq "github.com/bcowtech/lib-nsq"
)

func TestConsumer(t *testing.T) {

	config := nsq.NewConfig()
	{
		config.LookupdPollInterval = time.Second * 3
		config.DefaultRequeueDelay = 0
		config.MaxBackoffDuration = time.Millisecond * 50
		config.LowRdyIdleTimeout = time.Second * 1
		config.RDYRedistributeInterval = time.Millisecond * 20
	}

	c := &nsq.Consumer{
		NsqAddress:         os.Getenv("NSQLOOKUPD_ADDRESS"),
		Channel:            "gotest",
		HandlerConcurrency: 3,
		Config:             config,
		MessageHandler: nsq.MessageHandleProc(func(message *nsq.Message) error {
			fmt.Printf("[%s] %+v\n", message.Topic, string(message.Body))
			message.Finish()
			return nil
		}),
		UnhandledMessageHandler: nil,
	}

	ctx, _ := context.WithTimeout(context.Background(), 15*time.Second)

	err := c.Subscribe([]string{"myTopic"})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Consumer %+v", c)

	select {
	case <-ctx.Done():
		t.Logf("Consumer stopping")
		c.Close()
		return
	}
}
