package pl.cyfronet.dice.sad.cep;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;

import java.net.URI;
import java.net.URISyntaxException;

/**
* Created by tomek on 04.02.15.
*/
public class GenericComplexEventListener implements UpdateListener {

    Jedis jedis;

    public GenericComplexEventListener(String redisURI) throws URISyntaxException {
        URI uri = new URI(redisURI);
        JedisShardInfo info = new JedisShardInfo(uri);
        info.setTimeout(30000);
        jedis = new Jedis(info);
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        StringBuffer eventStrBuff = new StringBuffer("{\n");
        for (String propName : newEvents[0].getEventType().getPropertyNames()) {
            eventStrBuff.append(propName).append(": ").append(newEvents[0].get(propName)).append("\n");
        }
        jedis.publish("AtmosphereComplexEvent", eventStrBuff.append("}").toString());
    }
}
