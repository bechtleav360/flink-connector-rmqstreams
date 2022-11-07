package com.bechtle.maverick.insights.rmqstreams;

import com.bechtle.maverick.insights.rmqstreams.config.RMQStreamsConfig;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.Producer;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.rabbitmq.RMQSinkPublishOptions;
import org.apache.flink.streaming.connectors.rabbitmq.SerializableReturnListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

public class RMQStreamSink<IN> extends RichSinkFunction<IN> {
    private static final Logger LOG = LoggerFactory.getLogger(RMQStreamSink.class);
    private final RMQStreamsConfig config;
    private final SerializationSchema<IN> schema;
    private final RMQSinkPublishOptions<IN> publishOptions;
    private final SerializableReturnListener returnListener;
    private Environment environment;
    private Producer producer;

    public RMQStreamSink(RMQStreamsConfig config,
                         SerializationSchema<IN> schema,
                         @Nullable RMQSinkPublishOptions<IN> publishOptions,
                         @Nullable SerializableReturnListener returnListener) {
        this.config = config;
        this.schema = schema;
        this.publishOptions = publishOptions;
        this.returnListener = returnListener;
    }


    public RMQStreamSink(RMQStreamsConfig config,
                         SerializationSchema<IN> schema) {
        this(config, schema, null, null);
    }

    public RMQStreamSink(RMQStreamsConfig config,
                         String streamName,
                         SerializationSchema<IN> schema,
                         @Nullable RMQSinkPublishOptions<IN> publishOptions) {
        this(config, schema, publishOptions, null);
    }



    @Override
    public void invoke(IN value, Context context) throws Exception {


            byte[] body = this.schema.serialize(value);

            Message next = this.config.getMessageBuilder()
                    .addData(body)
                    .build();

            this.config.getProducer().send(next, confirmationStatus -> {
                if(!confirmationStatus.isConfirmed()) {
                    LOG.error("Failed to deliver message. Code: {}", confirmationStatus.getCode());
                } else {
                    LOG.debug("Delivered message with id '{}'", confirmationStatus.getMessage().getPublishingId());
                }
            });

    }
}
