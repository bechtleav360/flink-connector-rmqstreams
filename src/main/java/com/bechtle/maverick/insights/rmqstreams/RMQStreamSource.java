package com.bechtle.maverick.insights.rmqstreams;

import com.bechtle.maverick.insights.rmqstreams.extensions.RMQStreamsDeserializationSchema;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

public class RMQStreamSource<OUT> extends RichSourceFunction<OUT> {

    private final RMQConnectionConfig rmqConnectionConfig;
    private final String streamName;
    private final boolean usesCorrelationId;
    private final RMQStreamsDeserializationSchema<OUT> deliveryDeserializer;

    public RMQStreamSource(
            RMQConnectionConfig rmqConnectionConfig,
            String streamName,
            DeserializationSchema<OUT> deserializationSchema) {
        this(rmqConnectionConfig, streamName, false, deserializationSchema);
    }


    public RMQStreamSource(
            RMQConnectionConfig rmqConnectionConfig,
            String streamName,
            boolean usesCorrelationId,
            DeserializationSchema<OUT> deserializationSchema) {
        this.rmqConnectionConfig = rmqConnectionConfig;
        this.streamName = streamName;
        this.usesCorrelationId = usesCorrelationId;
        this.deliveryDeserializer = new RMQStreamsDeserializationSchema<>(deserializationSchema);
    }


    @Override
    public void run(SourceContext<OUT> sourceContext) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
