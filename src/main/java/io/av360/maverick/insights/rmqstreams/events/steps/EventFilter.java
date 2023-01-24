package io.av360.maverick.insights.rmqstreams.events.steps;

import io.cloudevents.CloudEvent;
import org.apache.flink.api.common.functions.FilterFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashSet;
import java.util.Set;

/**
 * Filters out events which don't match the given types
 */
public class EventFilter implements FilterFunction<CloudEvent> {
    private static final Logger LOG = LoggerFactory.getLogger(EventFilter.class);
    private final Set<String> supportedEventTypes;


    public EventFilter(Set<String> supportedEventTypes) {
        this.supportedEventTypes = new HashSet<>(supportedEventTypes);
    }

    public EventFilter() {
        this(Set.of());
    }

    @Override
    public boolean filter(CloudEvent value) throws Exception {
        if(value == null) {
            LOG.warn("Skipping null event.");
            return false;
        }

        if(value.getId() == null || value.getId().isEmpty()) {
            LOG.warn("Skipping event with missing identifier");
            return false;
        }

        if(value.getType() == null || value.getType().isEmpty()) {
            LOG.warn("Skipping event with id {} which is missing a type definition", value.getId());
            return false;
        }

        if(this.supportedEventTypes.contains(value.getType())) {
            LOG.info("Event of type '{}' will be consumed by this Job", value.getType());
            return true;
        } else {
            LOG.debug("Skipping event of type {} for this Job", value.getType());
            return false;
        }

    }
}
