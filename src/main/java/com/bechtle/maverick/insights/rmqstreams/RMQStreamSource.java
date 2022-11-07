package com.bechtle.maverick.insights.rmqstreams;

import com.bechtle.maverick.insights.rmqstreams.config.RMQStreamsConfig;
import com.bechtle.maverick.insights.rmqstreams.extensions.MessageCollector;
import com.bechtle.maverick.insights.rmqstreams.extensions.MessageDeserializationWrapper;
import com.bechtle.maverick.insights.rmqstreams.extensions.QueuedMessageHandler;
import com.rabbitmq.stream.*;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class RMQStreamSource<OUT> extends RichSourceFunction<OUT> {
    private static final Logger LOG = LoggerFactory.getLogger(RMQStreamSource.class);
    private final RMQStreamsConfig config;

    private final DeserializationSchema<OUT> deliveryDeserializer;

    private volatile boolean isRunning = true;

    public RMQStreamSource(
            RMQStreamsConfig config,
            DeserializationSchema<OUT> deserializationSchema) {
        this.config = config;
        this.deliveryDeserializer = new MessageDeserializationWrapper<>(deserializationSchema);
    }

    @Override
    public void run(SourceContext<OUT> sourceContext) throws Exception {
        QueuedMessageHandler<OUT> handler = new QueuedMessageHandler<>();
        MessageCollector<OUT> collector = new MessageCollector<>(sourceContext);

        try(Consumer ignored = this.config.getConsumer(handler)) {
            LOG.info("Starting consumer loop.");
            while(isRunning) {
                Message next = handler.next(sourceContext);  // blocking
                this.processMessage(next, collector);
            }
        } catch (Exception e) {
            LOG.error("Exception while consuming events from stream", e);
        }
    }

    private void processMessage(Message message, MessageCollector<OUT> collector) throws IOException {
        collector.extractParameter(message);
        this.deliveryDeserializer.deserialize(message.getBodyAsBinary(), collector);
    }

    @Override
    public void cancel() {
        this.isRunning = false;
    }
}
