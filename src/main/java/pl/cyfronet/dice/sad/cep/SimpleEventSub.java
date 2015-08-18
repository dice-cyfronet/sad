package pl.cyfronet.dice.sad.cep;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.fge.jackson.JsonLoader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.cyfronet.dice.sad.SADException;
import redis.clients.jedis.JedisPubSub;

import java.io.IOException;
import java.util.Map;

/**
 * Created by tomek on 8/5/15.
 */
public class SimpleEventSub extends JedisPubSub {

    private static final Logger log = LoggerFactory.getLogger(SimpleEventSub.class);
    private final ObjectMapper mapper;

    private Engine engine;

    public SimpleEventSub() {
        this.engine = Engine.getInstnace();
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
        log.info("Message: " + message);
        Map<String, Object> simpleEvent;
        try {
            simpleEvent = decodeMsgToSimpleEv(message);
            engine.sendEvent(
                    (Map) simpleEvent.get("properties"),
                    (String) simpleEvent.get("name")
            );
        } catch (SADException e) {
            log.warn(e.getMessage());
        }
    }

    private Map<String, Object> decodeMsgToSimpleEv(String msg) throws SADException {

        JsonNode json;
        try {
            json = JsonLoader.fromString(msg);
        } catch (IOException ioe) {
            throw new SADException(
                    String.format("Error while parsing JSON message: %s to Map", msg),
                    ioe
            );
        }
        return mapper.convertValue(json, Map.class);
    }
}
