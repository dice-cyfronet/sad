package pl.cyfronet.dice.sad.cep;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.cyfronet.dice.sad.Configurator;
import pl.cyfronet.dice.sad.SADException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisShardInfo;

import java.net.URI;
import java.net.URISyntaxException;

/**
* Created by tomek on 04.02.15.
*/
public class RedisComplexEventSink implements UpdateListener {

    private static RedisComplexEventSink instance;

    public static RedisComplexEventSink getInstance() throws SADException {
        return (instance != null) ? instance : (instance = new RedisComplexEventSink());
    }

    private static final Logger log = LoggerFactory.getLogger(RedisComplexEventSink.class);
    private Jedis jedis;

    private RedisComplexEventSink() throws SADException {
        Configurator configurator = Configurator.INSTANCE;
        String redisURI = configurator.getProperty("redisURI");
        URI uri;
        try {
            uri = new URI(redisURI);
        } catch (URISyntaxException e) {
            String exMsg = "Failed to create RedisComplexEventSink"
                    + " due to invalid Redis URI ("
                    + redisURI + "). Make sure it is defined in props file.";
            throw new SADException(exMsg, e);
        }
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
        String msg = eventStrBuff.append("}").toString();
        log.info("Complex event: " + msg);
        jedis.publish("AtmosphereComplexEvent", msg);
    }
}
