package io.av360.maverick.insights.rmqstreams.events;

import io.av360.maverick.insights.rmqstreams.RMQStreamSink;
import io.av360.maverick.insights.rmqstreams.config.RMQStreamsConfig;
import io.cloudevents.CloudEvent;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSinkPublishOptions;
import org.apache.flink.streaming.connectors.rabbitmq.SerializableReturnListener;

import javax.annotation.Nullable;

public class RMQEventStreamSink extends RMQStreamSink<CloudEvent> {

    public RMQEventStreamSink(RMQStreamsConfig config, @Nullable RMQSinkPublishOptions<CloudEvent> publishOptions, @Nullable SerializableReturnListener returnListener) {
        super(config, new CloudEventSerializer(), publishOptions, returnListener);
    }
}
