package io.av360.maverick.insights.rmqstreams;

import io.av360.maverick.insights.rmqstreams.config.RMQStreamsConfig;
import io.av360.maverick.insights.rmqstreams.extensions.MessageCollector;
import io.av360.maverick.insights.rmqstreams.extensions.MessageDeserializationWrapper;
import io.av360.maverick.insights.rmqstreams.extensions.QueuedMessageHandler;
import com.rabbitmq.stream.*;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

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
        this.parse(message, collector);
    }

    protected void parse(Message message, Collector<OUT> collector) throws IOException {
        this.deliveryDeserializer.deserialize(message.getBodyAsBinary(), collector);
    }



    @Override
    public void cancel() {
        this.isRunning = false;
    }
}
