package io.av360.maverick.insights.rmqstreams.extensions;

import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.Properties;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class MessageCollector<OUT> implements Collector<OUT> {

    private static final Logger LOG = LoggerFactory.getLogger(MessageCollector.class);
    private final SourceFunction.SourceContext<OUT> ctx;
    private long publishingId;
    private Map<String, Object> messageAnnotations;
    private Properties properties;

    public MessageCollector(SourceFunction.SourceContext<OUT> ctx) {
        this.ctx = ctx;
    }

    @Override
    public void collect(OUT out) {
        this.ctx.collect(out);
    }

    @Override
    public void close() {

    }

    public void extractParameter(Message message) {
            this.publishingId = message.getPublishingId();
            this.messageAnnotations = message.getMessageAnnotations();
            this.properties = message.getProperties();
    }

    /**
     * @see https://rabbitmq.github.io/rabbitmq-stream-java-client/stable/htmlsingle/#understanding-publishing-id
     * @return The publishing id
     */
    public long getPublishingId() {
        return publishingId;
    }

    /**
     *
     * @return a set of key/value pairs (aimed at the infrastructure).
     */
    public Map<String, Object> getMessageAnnotations() {
        return messageAnnotations;
    }

    /**
     *
     * @return a defined set of standard properties of the message (e.g. message ID, correlation ID, content type, etc).
     */
    public Properties getProperties() {
        return properties;
    }
}
