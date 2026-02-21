package kafka

import kafkago "github.com/segmentio/kafka-go"

// HeadersCarrier adapts []kafkago.Header to otel TextMapCarrier.
type HeadersCarrier struct{ Headers *[]kafkago.Header }

func (c HeadersCarrier) Get(key string) string {
	for _, h := range *c.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

func (c HeadersCarrier) Set(key, value string) {
	for i, h := range *c.Headers {
		if h.Key == key {
			(*c.Headers)[i].Value = []byte(value)
			return
		}
	}
	*c.Headers = append(*c.Headers, kafkago.Header{Key: key, Value: []byte(value)})
}

func (c HeadersCarrier) Keys() []string {
	keys := make([]string, len(*c.Headers))
	for i, h := range *c.Headers {
		keys[i] = h.Key
	}
	return keys
}
