package com.bechtle.maverick.insights.rmqstreams.config;

import com.bechtle.maverick.insights.rmqstreams.extensions.MessageCollector;
import com.bechtle.maverick.insights.rmqstreams.extensions.QueuedMessageHandler;
import com.rabbitmq.stream.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import javax.annotation.Nullable;
import java.io.Serializable;

public class RMQStreamsConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private final StreamsClientFactory factory;
    private final String streamOut;
    private final String streamIn;
    private final RMQConnectionConfig config;
    // private Environment environment;
    //private OffsetSpecification offsetSpecification;
    private boolean usesCorrelationId;
    //private Producer producer;

    public RMQStreamsConfig(RMQConnectionConfig config, String streamIn, @Nullable String streamOut) {
        this.config = config;
        this.factory = new StreamsClientFactory(config);
        this.streamIn = streamIn;
        this.streamOut = streamOut;
    }

    public RMQStreamsConfig(RMQConnectionConfig config, String stream) {
        this(config, stream, null);
    }

    public RMQConnectionConfig getConnectionConfig() {
        return config;
    }

    public Environment getEnvironment() {
        Environment environment = this.factory.buildEnvironment();
        environment.streamCreator().stream(this.streamIn).create();
        if(StringUtils.isNotEmpty(streamOut)) environment.streamCreator().stream(this.streamOut).create();

        return environment;
    }

    public Producer getProducer() {

            String stream = StringUtils.isNotEmpty(streamOut) ? streamOut : streamIn;
            Producer producer = this.factory.buildProducer(this.getEnvironment(), stream);

        return producer;
    }

    public Consumer getConsumer(QueuedMessageHandler<?> handler) {
        //OffsetSpecification offsetSpecification = this.offsetSpecification != null ? this.offsetSpecification : OffsetSpecification.last();
        OffsetSpecification offsetSpecification = OffsetSpecification.first();
        return this.factory.buildConsumer(this.getEnvironment(), handler, this.streamIn, offsetSpecification);
    }

//    public void setOffsetSpecification(OffsetSpecification offsetSpecification) {
//        this.offsetSpecification = offsetSpecification;
//    }

    public void setUsesCorrelationId(boolean usesCorrelationId) {
        this.usesCorrelationId = usesCorrelationId;
    }

    public MessageBuilder getMessageBuilder() {
        return this.getProducer().messageBuilder();
    }
}
