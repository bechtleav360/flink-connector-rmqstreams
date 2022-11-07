package com.bechtle.maverick.insights.rmqstreams.extensions;

import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.Properties;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.rabbitmq.RMQDeserializationSchema;
import org.apache.flink.util.Collector;

import java.util.Map;

public class MessageCollector<OUT> implements Collector<OUT> {

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

    public long getPublishingId() {
        return publishingId;
    }

    public Map<String, Object> getMessageAnnotations() {
        return messageAnnotations;
    }

    public Properties getProperties() {
        return properties;
    }
}
