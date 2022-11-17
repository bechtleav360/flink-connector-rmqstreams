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
import java.time.temporal.ChronoUnit;

public class StreamsClientFactory implements Serializable {
    private static final Logger LOG = LoggerFactory.getLogger(StreamsClientFactory.class);

    private static final long serialVersionUID = 1L;
    private RMQConnectionConfig config;
    private String stream;

    public StreamsClientFactory(RMQConnectionConfig config, @Nullable String stream) {
        if(config == null) throw new NullPointerException("Connection config must not be null");

        this.config = config;
        this.stream = stream;
    }

    public StreamsClientFactory(RMQConnectionConfig config) {
        this(config, null);
    }


    public Environment buildEnvironment() {
        LOG.debug("Building environment");

        this.validateConfiguration();

        EnvironmentBuilder environmentBuilder = Environment.builder()
                .host(config.getHost())
                .port(config.getPort())
                .username(config.getUsername())
                .password(config.getPassword());

        if(StringUtils.isNotEmpty(config.getVirtualHost())) environmentBuilder.virtualHost(config.getVirtualHost());
        if(config.getRequestedHeartbeat() != null) environmentBuilder.requestedHeartbeat(Duration.of(config.getRequestedHeartbeat(), ChronoUnit.SECONDS));


        try {
            Environment build = environmentBuilder.build();
            LOG.info("Environment created with configured host: '{}'", config.getHost());
            return build;
        } catch (Exception e) {
            LOG.error("Exception while creating environment with host: '{}'", config.getHost());
            throw e;
        }

    }

    private void validateConfiguration() {
        if(config == null) throw new NullPointerException("Connection config must not be null");

        String prefix = "Missing configuration parameter:";

        StringBuilder sb = new StringBuilder();

        if(StringUtils.isBlank(config.getHost())) sb.append(" Host");
        if(config.getPort() <= 0) sb.append(" Port");
        if(StringUtils.isBlank(config.getUsername())) sb.append(" Username");
        if(StringUtils.isBlank(config.getPassword())) sb.append(" Password");

        if(sb.length() > 0) {
            throw new NullPointerException(prefix + sb.toString());
        }

        LOG.info("Valid configuration for host '{}'", config.getHost());
    }

    public Producer buildProducer(Environment environment, String stream) {
        return environment.producerBuilder()
                .stream(stream)
                .build();
    }


    public Consumer buildConsumer(Environment environment, QueuedMessageHandler<?> messageHandler, String stream, OffsetSpecification offsetSpecification) {
        LOG.info("Creating consumer for environment '{}', stream '{}' and offset {}", environment.toString(), stream, offsetSpecification.getOffset());
        return environment.consumerBuilder()
                .stream(stream)
                .offset(offsetSpecification)
                .messageHandler(messageHandler)
                .build();
    }


}
