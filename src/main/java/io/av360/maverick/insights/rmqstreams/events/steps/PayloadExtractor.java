package io.av360.maverick.insights.rmqstreams.events.steps;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.cloudevents.CloudEvent;
import io.cloudevents.CloudEventData;
import io.cloudevents.jackson.JsonCloudEventData;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Extracts the event payload and maps it to an ObjectNode
 */
public class PayloadExtractor implements MapFunction<CloudEvent, ObjectNode> {
    private static final Logger LOG = LoggerFactory.getLogger(PayloadExtractor.class);

    @Override
    public ObjectNode map(CloudEvent value) throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();


        if(value.getData() == null) throw new IOException("No valid payload in event");

        try {
            JsonCloudEventData data = (JsonCloudEventData) value.getData();

            if(! data.getNode().isObject()) return objectMapper.createObjectNode();

            LOG.info("Valid event of type '{}' received", value.getType());

            ObjectNode result  = (ObjectNode) data.getNode();
            if(result != null && result.isObject()) {
                LOG.info("Valid Payload  with '{}' fields",result.size());
            }
            return result;

        } catch (Exception ex) {
            LOG.error("Failed to parse payload into json", ex);
            throw ex;
        }


    }
}
