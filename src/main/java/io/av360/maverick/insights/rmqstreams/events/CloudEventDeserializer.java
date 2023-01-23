package io.av360.maverick.insights.rmqstreams.events;

import com.rabbitmq.stream.Message;
import io.av360.maverick.insights.rmqstreams.extensions.MessageDeserializationWrapper;
import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventContext;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.cloudevents.core.provider.EventFormatProvider;
import io.cloudevents.jackson.JsonFormat;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

public class CloudEventDeserializer extends AbstractDeserializationSchema<CloudEvent>  {

    private static final Logger LOG = LoggerFactory.getLogger(CloudEventDeserializer.class);

    public CloudEventDeserializer() {

    }

    @Override
    public CloudEvent deserialize(byte[] message) throws IOException {
        return Objects.requireNonNull(EventFormatProvider.getInstance().resolveFormat(JsonFormat.CONTENT_TYPE)).deserialize(message);
    }

    public CloudEvent deserialize(Message msg) throws IOException {

        throw new NotImplementedException("Coming soon");
    }


    @Override
    public boolean isEndOfStream(CloudEvent nextElement) {
        return false;
    }





}
