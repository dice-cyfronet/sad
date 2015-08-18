package pl.cyfronet.dice.sad.cep;

import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.cyfronet.dice.sad.Configurator;
import pl.cyfronet.dice.sad.SADException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisShardInfo;

import java.net.URI;
import java.net.URISyntaxException;

/**
* Created by tomek on 04.02.15.
*/
public class RedisComplexEventSink implements UpdateListener {

    private static volatile RedisComplexEventSink instance;

    private JedisPool jedisPool;

    public static void createInstance(JedisPool pool) {
        if (instance  == null) {
            synchronized (RedisComplexEventSink.class) {
                if (instance == null) {
                    instance = new RedisComplexEventSink(pool);
                }
            }
        }
    }

    public static RedisComplexEventSink getInstance() {
        return instance;
    }

    private static final Logger log = LoggerFactory.getLogger(RedisComplexEventSink.class);

    private RedisComplexEventSink(JedisPool pool) {
        jedisPool = pool;
    }

    @Override
    public void update(EventBean[] newEvents, EventBean[] oldEvents) {
        Jedis jedis = jedisPool.getResource();
        StringBuffer eventStrBuff = new StringBuffer("{\n");
        for (String propName : newEvents[0].getEventType().getPropertyNames()) {
            eventStrBuff.append(propName).append(": ").append(newEvents[0].get(propName)).append("\n");
        }
        String msg = eventStrBuff.append("}").toString();
        log.info("Complex event: " + msg);

        jedis.publish("AtmosphereComplexEvent", msg);
        jedisPool.returnResource(jedis);
    }
}
