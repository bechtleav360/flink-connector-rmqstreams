package io.av360.maverick.insights.rmqstreams.events;

import io.cloudevents.CloudEvent;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;

import java.io.IOException;
import java.util.Objects;

public class  CloudEventSchema extends AbstractDeserializationSchema<CloudEvent> implements SerializationSchema<CloudEvent> {

    public CloudEventSchema() {
    }

    @Override
    public CloudEvent deserialize(byte[] message) throws IOException {
        return Objects.requireNonNull(EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE)).deserialize(message);
    }

    @Override
    public boolean isEndOfStream(CloudEvent nextElement) {
        return false;
    }

    @Override
    public void open(SerializationSchema.InitializationContext context) throws Exception {
        SerializationSchema.super.open(context);
    }

    @Override
    public byte[] serialize(CloudEvent element) {
        return Objects.requireNonNull(EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE)).serialize(element);
    }
}
