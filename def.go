package nsq

import (
	"log"
	"os"

	"github.com/nsqio/go-nsq"
)

const (
	SERVICE_NSQD       = "nsqd"
	SERVICE_NSQLOOKUPD = "nsqlookupd"

	LOGGER_PREFIX string = "[bcowtech/lib-nsq] "
)

var (
	logger *log.Logger = log.New(os.Stdout, LOGGER_PREFIX, log.LstdFlags|log.Lmsgprefix)
)

type (
	Config = nsq.Config

	MessageHandleProc = func(message *Message) error
)
