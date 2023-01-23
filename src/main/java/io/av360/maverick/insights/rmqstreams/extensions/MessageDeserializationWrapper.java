package io.av360.maverick.insights.rmqstreams.extensions;

import com.esotericsoftware.minlog.Log;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.Properties;
import io.av360.maverick.insights.rmqstreams.RMQStreamSource;
import io.av360.maverick.insights.rmqstreams.events.CloudEventDeserializer;
import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventAttributes;
import io.cloudevents.CloudEventContext;
import io.cloudevents.core.impl.BaseCloudEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MessageDeserializationWrapper<OUT> implements DeserializationSchema<OUT> {

    private final DeserializationSchema<OUT> schema;

    private static final Logger LOG = LoggerFactory.getLogger(MessageDeserializationWrapper.class);

    public MessageDeserializationWrapper(DeserializationSchema<OUT> deserializationSchema) {
        schema = deserializationSchema;
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return schema.getProducedType();
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        schema.open(context);
    }

    @Override
    public OUT deserialize(byte[] bytes) throws IOException {
        return this.schema.deserialize(bytes);
    }

    public void deserialize(Message message, Collector<OUT> out) throws IOException {
        if(this.schema instanceof CloudEventDeserializer) {
            LOG.warn("Deserializing binary amqp messages is not implemented yet, skipping message. ");
            // CloudEvent deserialize = ((CloudEventDeserializer) schema).deserialize(message);

        } else {
            LOG.warn("Skipping CloudEvent in binary mode. Wrong deserializer registered of type {}", schema.getClass().getSimpleName());
        }

    }

    @Override
    public void deserialize(byte[] message,  Collector<OUT> collector) throws IOException {
       this.schema.deserialize(message, collector);
    }

    @Override
    public boolean isEndOfStream(OUT out) {
        return false;
    }
}
