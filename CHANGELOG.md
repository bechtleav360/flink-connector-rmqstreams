
# 0.1
Initial version using the [RabbitMQ Stream](https://github.com/rabbitmq/rabbitmq-stream-java-client) java client. Various hotfixes
to get serialization as expected by Apache Flink. 

# 0.2
Adding Cloud Event support. Expecting [JSON-encoded](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/formats/json-format.md) cloud events in the message payload.
Includes support for filtering by event type. 


# 0.3 (Planned)
Support for the [AMQP Protocol](https://github.com/cloudevents/spec/blob/v1.0.2/cloudevents/bindings/amqp-protocol-binding.md) binding.
Cloudevent Properties are then part of AMQP [application-properties](http://docs.oasis-open.org/amqp/core/v1.0/os/amqp-core-messaging-v1.0-os.html#type-application-properties). 

Has the benefit, that filtering for certain event types can be handled before the message payload has to be deserialized. 

Includes support for additional filtering by additional cloud event properties (source, subject) 