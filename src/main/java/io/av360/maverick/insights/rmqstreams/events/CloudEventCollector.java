package io.av360.maverick.insights.rmqstreams.events;

import io.cloudevents.CloudEvent;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.Set;

public class CloudEventCollector implements Collector<CloudEvent> {

    private final Collector<CloudEvent> delegate;
    private final Set<String> filtered_types;

    public CloudEventCollector(Collector<CloudEvent> collector, @Nullable Set<String> filtered_types) {
        this.delegate = collector;
        this.filtered_types = filtered_types;
    }

    @Override
    public void collect(CloudEvent cloudEvent) {
        // no filters set -> everything is forwarded
        if( (filtered_types == null) || (filtered_types.size() == 0) ) {
            delegate.collect(cloudEvent);
        } else if(filtered_types.contains(cloudEvent.getType())) {
            delegate.collect(cloudEvent);
        }
    }

    @Override
    public void close() {
        this.delegate.close();
    }
}
