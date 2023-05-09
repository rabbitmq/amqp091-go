package amqp091

import (
	"go.opentelemetry.io/otel/propagation"
)

// PublishingCarrier is a TextMapCarrier that uses a Publishing.Headers as a storage
// medium for propagated key-value pairs.
type PublishingCarrier = Publishing

// Compile time check that PublishingCarrier implements the TextMapCarrier.
var _ propagation.TextMapCarrier = PublishingCarrier{}

func (p PublishingCarrier) Get(key string) string {
	v, ok := p.Headers[key]
	if !ok {
		return ""
	}
	return v.(string)
}

func (p PublishingCarrier) Set(key string, value string) {
	p.Headers[key] = value
}

func (p PublishingCarrier) Keys() []string {
	i := 0
	r := make([]string, len(p.Headers))

	for k, _ := range p.Headers {
		r[i] = k
		i++
	}

	return r
}
