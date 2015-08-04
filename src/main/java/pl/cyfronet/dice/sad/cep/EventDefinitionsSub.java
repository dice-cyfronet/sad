package pl.cyfronet.dice.sad.cep;

import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import pl.cyfronet.dice.sad.SADException;
import redis.clients.jedis.JedisPubSub;


import java.util.Map;

/**
 * Created by tomek on 8/3/15.
 */
public class EventDefinitionsSub extends JedisPubSub {

    private Engine engine;
    private JSONParser parser;

    EventDefinitionsSub(Engine engine) {
        this.engine = engine;
        parser = new JSONParser();
    }

    @Override
    public void onUnsubscribe(String channel, int subscribedChannels) {
        System.out.println("onUnsubscribe");
    }

    @Override
    public void onSubscribe(String channel, int subscribedChannels) {
        System.out.println("onSubscribe");
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
        System.out.println("Message: " + message);
        Map<String, Object> evDefs = decodeMsgToEvenetDef(message);
        try {
            validateEventDefinitions(evDefs);
        } catch (SADException e) {
            e.printStackTrace();
        }
        if (evDefs == null) { return; }
        for (Map.Entry<String, Object> e : evDefs.entrySet()) {
            System.out.println("Key " + e.getKey() + ", value " + e.getValue());
        }
    }

    private boolean validateEventDefinitions(Map<String, Object> evDefs) throws SADException {
        // TODO not empty, contains a valid simple event definition
        if (evDefs.isEmpty()) {
            System.out.println("Provided event definition is empty");
            return false;
        }
        return true;
    }

    private Map<String, Object> decodeMsgToEvenetDef(String msg) {
        try {
            return (Map) parser.parse(msg);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return null;
    }

}
