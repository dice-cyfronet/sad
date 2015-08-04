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
import java.util.Map;

/**
 * Created by tomek on 8/3/15.
 */
public class EventDefinitionsSub extends JedisPubSub {

    private static final Logger log = LoggerFactory.getLogger(EventDefinitionsSub.class);

    private final Engine engine;

    private final JsonNode msgSchema;
    private final JsonSchemaFactory factory = JsonSchemaFactory.byDefault();
    private final JsonSchema schema;
    private final ObjectMapper mapper;

    EventDefinitionsSub(Engine engine) throws IOException, ProcessingException {
        this.engine = engine;
        msgSchema = JsonLoader.fromResource("/event_defs_schema.json");
        schema = factory.getJsonSchema(msgSchema);
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
        } catch (SADException e) {
            e.printStackTrace();
        }
        if (evDefs == null) { return; }
        for (Map.Entry<String, Object> e : evDefs.entrySet()) {
            log.info("Key " + e.getKey() + ", value " + e.getValue());
        }
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
