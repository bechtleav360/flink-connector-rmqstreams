package io.av360.maverick.insights.rmqstreams.events;

import io.av360.maverick.insights.rmqstreams.RMQStreamSource;
import io.av360.maverick.insights.rmqstreams.config.RMQStreamsConfig;
import com.rabbitmq.stream.Message;
import io.cloudevents.CloudEvent;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Set;

public class RMQEventStreamSource extends RMQStreamSource<CloudEvent> {

    private Set<String> filtered_types;

    public RMQEventStreamSource(RMQStreamsConfig config, @Nullable Set<String> filtered_types) {
        super(config, new CloudEventDeserializer());
        this.filtered_types = filtered_types;
    }

    public RMQEventStreamSource(RMQStreamsConfig config) {
        this(config, Set.of());
    }


    @Override
    protected void processMessage(Message message, Collector<CloudEvent> collector) throws IOException {
        CloudEventCollector cloudEventCollector = new CloudEventCollector(collector, filtered_types);
        super.processMessage(message, cloudEventCollector);
    }

}
