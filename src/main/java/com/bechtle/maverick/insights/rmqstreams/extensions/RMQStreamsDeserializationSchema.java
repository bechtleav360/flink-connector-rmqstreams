package com.bechtle.maverick.insights.rmqstreams.extensions;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Envelope;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.rabbitmq.RMQDeserializationSchema;

import java.io.IOException;

public class RMQStreamsDeserializationSchema<OUT> implements RMQDeserializationSchema<OUT> {

    private final DeserializationSchema<OUT> schema;

    public RMQStreamsDeserializationSchema(DeserializationSchema<OUT> deserializationSchema) {
        schema = deserializationSchema;
    }

    @Override
    public void deserialize(Envelope envelope, AMQP.BasicProperties properties, byte[] body, RMQCollector<OUT> collector) throws IOException {
        collector.collect(schema.deserialize(body));
    }

    @Override
    public boolean isEndOfStream(OUT nextElement) {
        return false;
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) throws Exception {
        schema.open(context);
    }

    @Override
    public TypeInformation<OUT> getProducedType() {
        return schema.getProducedType();
    }
}
