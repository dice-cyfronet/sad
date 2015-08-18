package pl.cyfronet.dice.sad.cep;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.cyfronet.dice.sad.SADException;
import redis.clients.jedis.JedisPubSub;

import java.util.Map;

/**
 * Created by tomek on 8/5/15.
 */
public class SimpleEventSub extends JedisPubSub {

    private static final Logger log = LoggerFactory.getLogger(SimpleEventSub.class);

    private Engine engine;

    public SimpleEventSub() {
        this.engine = Engine.getInstnace();
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
        Map<String, Object> simpleEvent = null;
        try {
            simpleEvent = decodeMsgToSimpleEv(message);

        } catch (SADException e) {
            log.warn(e.getMessage());
        }
    }

    private Map<String, Object> decodeMsgToSimpleEv(String message) throws SADException {

        return null;
    }
}
