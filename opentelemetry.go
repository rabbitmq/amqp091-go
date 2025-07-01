package amqp091

import (
	"context"
	"errors"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.25.0"
	"go.opentelemetry.io/otel/trace"
)

// tracer is the tracer used by the package
var tracer = otel.Tracer("amqp091")

// amqpHeaderCarrier is a carrier for AMQP headers.
type amqpHeaderCarrier Table

// Get returns the value associated with the passed key.
func (c amqpHeaderCarrier) Get(key string) string {
	v, ok := c[key]
	if !ok {
		return ""
	}
	s, ok := v.(string)
	if ok {
		return s
	}
	return ""
}

// Set stores the key-value pair.
func (c amqpHeaderCarrier) Set(key, value string) {
	c[key] = value
}

// Keys lists the keys stored in this carrier.
func (c amqpHeaderCarrier) Keys() []string {
	var keys []string
	for k, v := range c {
		if _, ok := v.(string); !ok {
			continue
		}
		keys = append(keys, k)
	}
	return keys
}

// ensure amqpHeaderCarrier implements the TextMapCarrier interface
var _ propagation.TextMapCarrier = amqpHeaderCarrier{}

// keys for conventions in this file
var (
	// settleResponseKey is the key for indicating how the message was settled
	settleResponseKey = attribute.Key("messaging.settle.response_type")
	// settleMultipleKey indicates whether multiple outstanding messages were settled at once.
	settleMultipleKey = attribute.Key("messaging.settle.multiple")
	// settleRequeueKey indicates whether the messages were requeued.
	settleRequeueKey = attribute.Key("messaging.settle.requeue")
	// publishImmediate key indicates whether the AMQP immediate flag was set on the publishing.
	publishImmediateKey = attribute.Key("messaging.publish.immediate")
	// returnOperation indicates an AMQP 091 return
	returnOperation = semconv.MessagingOperationKey.String("return")

	// Older Convention for storing Routing Key. Nodejs instrumentation-amqplib still uses this
	OldMessagingRabbitmqRoutingKey = attribute.Key("messaging.rabbitmq.routing_key")
)

// InjectSpan injects the span context into the AMQP headers.
// It returns the input headers with the span headers added.
func injectSpanFromContext(ctx context.Context, headers Table) Table {
	carrier := amqpHeaderCarrier(headers)
	if carrier == nil {
		carrier = amqpHeaderCarrier{}
	}
	otel.GetTextMapPropagator().Inject(ctx, carrier)
	return Table(carrier)
}

// ExtractSpanContext returns a new Context with span attributes set based on AMQP headers.
func ExtractSpanContext(ctx context.Context, headers Table) context.Context {
	carrier := amqpHeaderCarrier(headers)
	if carrier == nil {
		carrier = amqpHeaderCarrier{}
	}
	return otel.GetTextMapPropagator().Extract(ctx, carrier)
}

// extractSpanFromReturn creates a span for a returned message
func extractSpanFromReturn(
	ctx context.Context,
	ret Return,
	options ...trace.SpanStartOption,
) (context.Context, trace.Span) {
	spctx := ExtractSpanContext(ctx, ret.Headers)
	spanName := fmt.Sprintf("return %s %s", ret.Exchange, ret.RoutingKey)

	return tracer.Start(ctx, spanName,
		append(options,
			trace.WithLinks(trace.LinkFromContext(spctx, semconv.MessagingMessageID(ret.MessageId))),
			trace.WithSpanKind(trace.SpanKindProducer),
			trace.WithAttributes(
				OldMessagingRabbitmqRoutingKey.String(ret.RoutingKey),
				semconv.MessagingRabbitmqDestinationRoutingKey(ret.RoutingKey),
				semconv.MessagingDestinationPublishName(ret.Exchange),
				returnOperation,
				semconv.MessagingMessageID(ret.MessageId),
				semconv.MessagingMessageConversationID(ret.CorrelationId),
				semconv.MessagingSystemRabbitmq,
				semconv.ErrorTypeKey.String(ret.ReplyText),
				// semconv.NetPeerPort(5672
				// semconv.NetPeerIP("localhost")
				// semconv.ServerAddress("localhost")
			),
			trace.WithNewRoot(),
		)...,
	)
}

// settleAckDelivery creates a span for the acking of a delivery
func settleAckDelivery(
	ctx context.Context,
	delivery *Delivery,
	multiple bool,
) (context.Context, trace.Span) {
	return tracer.Start(ctx,
		fmt.Sprintf("settle %s %s", delivery.Exchange, delivery.RoutingKey),
		trace.WithAttributes(
			semconv.MessagingOperationSettle,
			settleResponseKey.String("ack"),
			settleMultipleKey.Bool(multiple),
		),
	)
}

// settleRejectDelivery creates a span for the rejection of a delivery
func settleRejectDelivery(
	ctx context.Context,
	delivery *Delivery,
	requeue bool,
) (context.Context, trace.Span) {
	return tracer.Start(ctx,
		fmt.Sprintf("settle %s %s", delivery.Exchange, delivery.RoutingKey),
		trace.WithAttributes(
			semconv.MessagingOperationSettle,
			settleResponseKey.String("reject"),
			settleRequeueKey.Bool(requeue),
		),
	)
}

// settleNackDelivery creates a span for the nacking of a delivery
func settleNackDelivery(
	ctx context.Context,
	delivery *Delivery,
	multiple, requeue bool,
) (context.Context, trace.Span) {
	return tracer.Start(ctx,
		fmt.Sprintf("settle %s %s", delivery.Exchange, delivery.RoutingKey),
		trace.WithAttributes(
			semconv.MessagingOperationSettle,
			settleResponseKey.String("nack"),
			settleMultipleKey.Bool(multiple),
			settleRequeueKey.Bool(requeue),
		),
	)
}

// extractLinkFromDelivery creates a link for a delivered message
//
// The recommend way to link a consumer to the publisher is with a link, since
// the two operations can be quit far apart in time. If you have a usecase
// where you would like the spans to have a parent child relationship instead, use
// ExtractSpanContext
//
// The consumer span may containe 1 or more messages, which is why we don't
// manufacture the span in its entirety here.
func extractLinkFromDelivery(ctx context.Context, del *Delivery) trace.Link {
	spctx := ExtractSpanContext(ctx, del.Headers)
	return trace.LinkFromContext(spctx,
		semconv.MessagingMessageConversationID(del.CorrelationId),
		semconv.MessagingMessageID(del.MessageId),
		semconv.MessagingRabbitmqMessageDeliveryTag(int(del.DeliveryTag)))
}

// AddDeliveryAttributes adds OpenTelemetry attributes to the provided span for a consumed message.
// It annotates the span with details such as the queue name, exchange, message ID, correlation ID,
// application ID, message body size, and marks the operation as a message delivery.
// This function is intended to be used when consuming messages from a RabbitMQ queue to provide
// rich telemetry data for observability.
func AddDeliveryAttributes(span trace.Span, data Delivery, queueName string) {
	span.SetAttributes(
		OldMessagingRabbitmqRoutingKey.String(queueName),
		semconv.MessagingRabbitmqDestinationRoutingKey(queueName),
		semconv.MessagingDestinationPublishName(data.Exchange),
		semconv.MessagingOperationDeliver,
		semconv.MessagingMessageID(data.MessageId),
		semconv.MessagingMessageConversationID(data.CorrelationId),
		semconv.MessagingSystemRabbitmq,
		semconv.MessagingClientIDKey.String(data.AppId),
		semconv.MessagingMessageBodySize(len(data.Body)),
		semconv.MessageTypeReceived,
	)
}

// spanForDelivery creates a span for the delivered messages
// returns a new context with the span headers and the span.
func spanForDelivery(ctx context.Context, delivery *Delivery, options ...trace.SpanStartOption) (context.Context, trace.Span) {
	spanName := fmt.Sprintf("consume %s %s", delivery.Exchange, delivery.RoutingKey)
	var links []trace.Link
	links = append(links, extractLinkFromDelivery(ctx, delivery))
	return tracer.Start(
		ctx,
		spanName,
		append(
			options,
			trace.WithLinks(links...),
			trace.WithSpanKind(trace.SpanKindConsumer),
		)...,
	)
}

// Publish creates a span for a publishing message returns a new context with
// the span headers, the mssage that was being published with span headers
// injected, and a function to be called with the result of the publish
func spanForPublication(
	ctx context.Context,
	publishing Publishing,
	exchange, routinKey string,
	immediate bool,
) (context.Context, Publishing, func(err error)) {
	spanName := fmt.Sprintf("%s publish", routinKey)
	ctx, span := tracer.Start(ctx, spanName,
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			OldMessagingRabbitmqRoutingKey.String(routinKey),
			semconv.MessagingRabbitmqDestinationRoutingKey(routinKey),
			semconv.MessagingDestinationPublishName(exchange),
			semconv.MessagingOperationPublish,
			semconv.MessagingMessageID(publishing.MessageId),
			semconv.MessagingMessageConversationID(publishing.CorrelationId),
			semconv.MessagingSystemRabbitmq,
			semconv.MessagingClientIDKey.String(publishing.AppId),
			semconv.MessagingMessageBodySize(len(publishing.Body)),
			publishImmediateKey.Bool(immediate),
			semconv.MessageTypeSent,
		),
	)
	headers := injectSpanFromContext(ctx, publishing.Headers)
	publishing.Headers = headers

	return ctx, publishing, func(err error) {
		if err != nil {
			span.RecordError(err)
			amqpErr := &Error{}
			if errors.As(err, &amqpErr) {
				span.SetAttributes(
					semconv.ErrorTypeKey.String(amqpErr.Reason),
				)
			}
			span.SetStatus(codes.Error, err.Error())
		}
		span.End()
	}
}
