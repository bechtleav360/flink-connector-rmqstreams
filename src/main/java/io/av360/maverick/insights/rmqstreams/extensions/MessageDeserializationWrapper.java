package io.av360.maverick.insights.rmqstreams.extensions;

import com.rabbitmq.stream.Message;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class MessageDeserializationWrapper<OUT> implements DeserializationSchema<OUT> {

    private final DeserializationSchema<OUT> schema;

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

    @Override
    public void deserialize(byte[] message, Collector<OUT> out) throws IOException {
        OUT deserialize = this.deserialize(message);
        if(deserialize != null) out.collect(deserialize);
    }

    public void deserialize(Message message, Collector<OUT> collector) throws IOException {
       this.deserialize(message.getBodyAsBinary(), collector);
    }

    @Override
    public boolean isEndOfStream(OUT out) {
        return false;
    }
}
