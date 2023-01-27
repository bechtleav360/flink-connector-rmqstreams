# flink-connector-rmqstreams

This a Flink Connector (Source and Sink) for [RabbitMQ Streams](https://www.rabbitmq.com/streams.html).

The [standard connector](https://nightlies.apache.org/flink/flink-docs-master/docs/connectors/datastream/rabbitmq/) 
builds on the RabbitMQ [standard library](https://www.rabbitmq.com/java-client.html) and enforces auto acknowledgements.  
This connector uses the [RabbitMQ Stream](https://github.com/rabbitmq/rabbitmq-stream-java-client) java client. 

See "develop"-branch for new updates. 

## Example for setting up a stream (with CloudEvents)

````java
// Port is 5552 for the streams module
final RMQConnectionConfig connectionConfig = new RMQConnectionConfig.Builder()
                .setHost("rabbitmq")
                .setPort(5552) 
                .setUserName("xxx")
                .setPassword("xxx")
                .build();


final RMQStreamsConfig streamsConfig = new RMQStreamsConfig(connectionConfig, "name of your events stream");

// we publish CloudEvents in our stream
final RMQStreamSource<CloudEvent> source = new RMQStreamSource<CloudEvent>(streamsConfig, new CloudEventSchema());

SingleOutputStreamOperator<CloudEvent> incoming = env.addSource(source)
                .returns(Types.GENERIC(CloudEvent.class));

// you can filter the incoming stream by event type 
incoming.filter(new EventFilter(Set.of("my.event.type")))
  .map( ...

````
