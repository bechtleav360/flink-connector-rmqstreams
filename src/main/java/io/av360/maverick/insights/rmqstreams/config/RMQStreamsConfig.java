package io.av360.maverick.insights.rmqstreams.config;

import io.av360.maverick.insights.rmqstreams.extensions.QueuedMessageHandler;
import com.rabbitmq.stream.*;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.time.Duration;

public class RMQStreamsConfig implements Serializable {

    private static final Logger LOG = LoggerFactory.getLogger(RMQStreamsConfig.class);

    private static final long serialVersionUID = 1L;
    private final String streamOut;
    private final String streamIn;
    private final RMQConnectionConfig connectionConfig;
    private final OffsetSpecification offsetSpecification = OffsetSpecification.first();
    private boolean usesCorrelationId;

    public RMQStreamsConfig(RMQConnectionConfig config, String streamIn, @Nullable String streamOut) {
        this.connectionConfig = config;
        this.streamIn = streamIn;
        this.streamOut = streamOut;
    }

    public RMQStreamsConfig(RMQConnectionConfig config, String stream) {
        this(config, stream, null);
    }

    public RMQConnectionConfig getConnectionConfig() {
        return connectionConfig;
    }

    private void validateStream(Environment environment, String stream) {

        try {
            LOG.info("Checking stream with name '{}'", this.streamIn);
            StreamStats streamStats = environment.queryStreamStats(this.streamIn);
        } catch (Exception e) {
            LOG.info("Stream does not exist, trying to create stream with name '{}'", this.streamIn);
            environment.streamCreator().stream(this.streamIn)
                    .maxAge(Duration.ofHours(24))
                    .maxSegmentSizeBytes(ByteCapacity.MB(500))
                    .create();
        }
    }

    public Environment getEnvironment() {
        LOG.info("Creating environment required to connect to a RabbitMQ Stream.");
        Environment environment = new StreamsClientFactory(this.getConnectionConfig()).buildEnvironment();

        this.validateStream(environment, this.streamIn);

        if (StringUtils.isNotEmpty(streamOut)) {
            this.validateStream(environment, this.streamOut);
        }

        return environment;
    }

    public Producer getProducer() {

        String stream = StringUtils.isNotEmpty(streamOut) ? streamOut : streamIn;
        return new StreamsClientFactory(this.getConnectionConfig()).buildProducer(this.getEnvironment(), stream);
    }

    public Consumer getConsumer(QueuedMessageHandler<?> handler) {
        LOG.info("Creating consumer for stream '{}'", this.streamIn);
        OffsetSpecification offsetSpecification = this.offsetSpecification != null ? this.offsetSpecification : OffsetSpecification.first();
        return new StreamsClientFactory(this.getConnectionConfig()).buildConsumer(this.getEnvironment(), handler, this.streamIn, offsetSpecification);
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
