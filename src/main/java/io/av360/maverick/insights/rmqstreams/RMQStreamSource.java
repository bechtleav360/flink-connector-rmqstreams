package io.av360.maverick.insights.rmqstreams;

import io.av360.maverick.insights.rmqstreams.config.RMQStreamsConfig;
import io.av360.maverick.insights.rmqstreams.extensions.MessageCollector;
import io.av360.maverick.insights.rmqstreams.extensions.MessageDeserializationWrapper;
import io.av360.maverick.insights.rmqstreams.extensions.QueuedMessageHandler;
import com.rabbitmq.stream.*;
import io.cloudevents.jackson.JsonFormat;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.shaded.guava30.com.google.common.net.MediaType;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

public class RMQStreamSource<OUT> extends RichSourceFunction<OUT> {
    private static final Logger LOG = LoggerFactory.getLogger(RMQStreamSource.class);
    private final RMQStreamsConfig config;

    private final MessageDeserializationWrapper<OUT> deliveryDeserializer;

    private volatile boolean isRunning = true;

    public RMQStreamSource(
            RMQStreamsConfig config,
            DeserializationSchema<OUT> deserializationSchema) {
        this.config = config;
        this.deliveryDeserializer = new MessageDeserializationWrapper<>(deserializationSchema);
    }

    @Override
    public void run(SourceContext<OUT> sourceContext) throws Exception {
        LOG.info("Running event consumer as stream source");

        QueuedMessageHandler<OUT> handler = new QueuedMessageHandler<>();
        MessageCollector<OUT> collector = new MessageCollector<>(sourceContext);

        try(Consumer ignored = this.config.getConsumer(handler)) {
            LOG.info("Starting consumer loop ...");
            while(isRunning) {
                Message next = handler.next(sourceContext);  // blocking until timeout, then null
                if(next != null) {
                    this.processMessage(next, collector);
                }

            }
        } catch (Exception e) {
            LOG.error("Exception while consuming events from stream", e);
        }
    }

    protected void processMessage(Message message, Collector<OUT> collector) throws IOException {
        if(Objects.isNull(message)) {
            LOG.warn("Skipping null message");
            return;
        }

        if(Objects.isNull(message.getProperties())) {
            LOG.warn("Skipping message without properties");
            return;
        }

        String contentType = message.getProperties().getContentType();
        if(StringUtils.isNullOrWhitespaceOnly(contentType)) {
            LOG.warn("Skipping message without valid content type in properties");
            return;
        }

        // binary content mode
        if(contentType.contentEquals(MediaType.JSON_UTF_8.toString())) {
            LOG.trace("Consuming CloudEvent in binary content mode.");
            this.parseBinary(message, collector);
        // Structured Content Mode
        } else if(contentType.contentEquals(JsonFormat.CONTENT_TYPE)) {
            LOG.trace("Consuming CloudEvent in structured content mode.");
            this.parseStructured(message, collector);
        } else {
            LOG.warn("Skipping event with content-type '{}', not a cloud event", contentType);
        }


    }

    protected void parseBinary(Message message, Collector<OUT> collector) throws IOException {
        this.deliveryDeserializer.deserialize(message, collector);
    }


    protected void parseStructured(Message message, Collector<OUT> collector) throws IOException {
        if(Objects.isNull(message.getBody())) {
            LOG.warn("Skipping CloudEvent in structured content mode with no payload.");
            return;
        }

        this.deliveryDeserializer.deserialize(message.getBodyAsBinary(), collector);

    }



    @Override
    public void cancel() {
        this.isRunning = false;
    }
}
