package com.bechtle.maverick.insights.rmqstreams.config;

import com.bechtle.maverick.insights.rmqstreams.extensions.QueuedMessageHandler;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.OffsetSpecification;
import com.rabbitmq.stream.Producer;
import org.apache.flink.streaming.connectors.rabbitmq.common.RMQConnectionConfig;

import javax.annotation.Nullable;
import java.io.Serializable;
import java.time.Duration;
import java.time.temporal.ChronoUnit;

public class StreamsClientFactory implements Serializable {
    private static final long serialVersionUID = 1L;
    private RMQConnectionConfig config;
    private String stream;

    public StreamsClientFactory(RMQConnectionConfig config, @Nullable String stream) {
        this.config = config;
        this.stream = stream;
    }

    public StreamsClientFactory(RMQConnectionConfig config) {
        this(config, null);
    }


    public Environment buildEnvironment() {
        return Environment.builder()
                .host(config.getHost())
                .port(config.getPort())
                .virtualHost(config.getVirtualHost())
                .username(config.getUsername())
                .password(config.getPassword())
                .requestedHeartbeat(Duration.of(config.getRequestedHeartbeat(), ChronoUnit.SECONDS))
                .build();
    }

    public Producer buildProducer(Environment environment, String stream) {
        return environment.producerBuilder()
                .stream(stream)
                .build();
    }


    public Consumer buildConsumer(Environment environment, QueuedMessageHandler<?> messageHandler, String stream, OffsetSpecification offsetSpecification) {
        return environment.consumerBuilder()
                .stream(stream)
                .offset(offsetSpecification)
                .messageHandler(messageHandler)
                .build();
    }


}
