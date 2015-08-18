package pl.cyfronet.dice.sad.cep;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jackson.JsonLoader;
import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import com.github.fge.jsonschema.core.report.ProcessingReport;
import com.github.fge.jsonschema.main.JsonSchema;
import com.github.fge.jsonschema.main.JsonSchemaFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.cyfronet.dice.sad.SADException;
import redis.clients.jedis.JedisPubSub;


import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by tomek on 8/3/15.
 */
public class EventDefinitionsSub extends JedisPubSub {

    private static volatile EventDefinitionsSub instance;

    private static final Logger log = LoggerFactory.getLogger(EventDefinitionsSub.class);

    private final Engine engine;

    private final JsonNode msgSchema;
    private final JsonSchemaFactory factory = JsonSchemaFactory.byDefault();
    private final JsonSchema schema;
    private final ObjectMapper mapper;

    public static EventDefinitionsSub getInstance() throws SADException {
        if (instance  == null) {
            synchronized (EventDefinitionsSub.class) {
                if (instance == null) {
                    instance = new EventDefinitionsSub();
                }
            }
        }
        return instance;
    }

    private EventDefinitionsSub() throws SADException {
        this.engine = Engine.getInstnace();
        try {
            msgSchema = JsonLoader.fromResource("/event_defs_schema.json");
            schema = factory.getJsonSchema(msgSchema);
        } catch (IOException e) {
            throw new SADException(
                    "Error creating EventDefinitionSub instance because event_defs_schema.json could not be loaded",
                    e
            );
        } catch (ProcessingException pe) {
            throw new SADException(
                    "Error creating EventDefinitionsSub instance because event_defs_schema.json could not be processed",
                    pe
            );
        }

        mapper = new ObjectMapper();
    }

    @Override
    public void onUnsubscribe(String channel, int subscribedChannels) {
        log.info("onUnsubscribe");
    }

    @Override
    public void onSubscribe(String channel, int subscribedChannels) {
        log.info("onSubscribe");
    }

    @Override
    public void onPUnsubscribe(String pattern, int subscribedChannels) {
    }

    @Override
    public void onPSubscribe(String pattern, int subscribedChannels) {
    }

    @Override
    public void onPMessage(String pattern, String channel, String message) {
    }

    @Override
    public void onMessage(String channel, String message) {
        //TODO parse event definition and add event type
        log.info("Message: " + message);
        Map<String, Object> evDefs = null;
        try {
            evDefs = decodeMsgToEventDefs(message);
            Map<String, Object> simpleEvDef = (Map<String, Object>) evDefs.get("simple_event");
            registerSimpleEvent(simpleEvDef);
            String eplStatementString = (String) evDefs.get("complex_event");
            registerComplexEventListener(eplStatementString);
        } catch (SADException e) {
            log.warn(e.getMessage());
        }
        if (evDefs == null) { return; }
        for (Map.Entry<String, Object> e : evDefs.entrySet()) {
            log.info("Key " + e.getKey() + ", value " + e.getValue());
        }
    }

    private void registerComplexEventListener(String eplStatementString) throws SADException {
        engine.subscribe(eplStatementString, RedisComplexEventSink.getInstance());
    }

    private void registerSimpleEvent(Map<String, Object> simpleEvDef) throws SADException {
        String simpleEventName = (String) simpleEvDef.get("name");
        Map<String, String> simpleEvProps = (Map<String, String>) simpleEvDef.get("properties");
        Map<String, Object> eventProps = new HashMap<>();
        for(Map.Entry<String, String> e : simpleEvProps.entrySet()) {
            try {
                eventProps.put(e.getKey(), Class.forName(e.getValue()));
            } catch (ClassNotFoundException cnfe) {
                throw new SADException(
                        String.format(
                                "Error while creating simple event definition. Class %s of property %s was not found",
                                e.getValue(),
                                e.getKey()
                        )
                );
            }
        }
        engine.addEventType(simpleEventName, eventProps);
    }

    private Map<String, Object> decodeMsgToEventDefs(String msg) throws SADException {
        try {
            JsonNode json = JsonLoader.fromString(msg);
            ProcessingReport report = schema.validate(json);
            if (report.isSuccess()) {
                return mapper.convertValue(json, Map.class);
            } else {
                throw new SADException(
                        String.format("Provided JSON message: %s is not valid", msg)
                );
            }
        } catch (IOException ioe) {
            throw new SADException(
                    String.format("Error while parsing JSON message: %s to Map", msg),
                    ioe
            );
        } catch (ProcessingException pe) {
            throw new SADException(
                    String.format("Error while validating JSON message: %s with schema: %s", msg, schema),
                    pe
            );
        }
    }

}
