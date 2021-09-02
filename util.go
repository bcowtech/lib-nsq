package nsq

import "strings"

func assert(condition bool, message string) {
	if !condition {
		panic(message)
	}
}

func SplitConnectionString(input string) (service string, addresses []string) {
	if len(input) > 0 {
		for _, service := range []string{
			SERVICE_NSQD,
			SERVICE_NSQLOOKUPD,
		} {
			if strings.HasPrefix(input, service+":") {
				var str = input[len(service)+1:]
				return service, strings.Split(str, ",")
			}
		}
	}
	return "", nil
}
