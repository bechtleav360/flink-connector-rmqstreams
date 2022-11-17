package io.av360.maverick.insights.rmqstreams.extensions;

import org.apache.flink.api.common.serialization.SerializationSchema;

public class MessageSerializationWrapper<IN> implements SerializationSchema<IN> {

    private SerializationSchema<IN> schema;

    public MessageSerializationWrapper(SerializationSchema<IN> schema) {
        this.schema = schema;
    }

    @Override
    public void open(InitializationContext context) throws Exception {
        schema.open(context);
    }

    @Override
    public byte[] serialize(IN in) {
        return this.schema.serialize(in);
    }
}
