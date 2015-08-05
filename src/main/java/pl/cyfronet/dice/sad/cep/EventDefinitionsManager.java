package pl.cyfronet.dice.sad.cep;

import com.github.fge.jsonschema.core.exceptions.ProcessingException;
import pl.cyfronet.dice.sad.Configurator;
import pl.cyfronet.dice.sad.SADException;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.JedisShardInfo;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * Created by tomek on 8/3/15.
 */
public class EventDefinitionsManager {
    private Engine engine;
    private Jedis jedis;
    private String evDefChannel;
    private JedisPubSub evDefSub;

    public EventDefinitionsManager(Engine engine) throws SADException {
        this.engine = engine;
        Configurator configurator = Configurator.INSTANCE;
        String redisURI = configurator.getProperty("redisURI");
        URI uri;
        try {
            uri = new URI(redisURI);
        } catch (URISyntaxException e) {
            String exMsg = "Failed to create EventDefinitionsManager"
                    + " due to invalid Redis URI ("
                    + redisURI + "). Make sure redisURI is defined in props file";
            throw new SADException(exMsg, e);
        }
        JedisShardInfo info = new JedisShardInfo(uri);
        info.setTimeout(30000);
        jedis = new Jedis(info);
        evDefChannel = configurator.getProperty("eventDefinitionChannel");
        evDefSub = EventDefinitionsSub.getInstance(engine);
    }

    public void start() {
        jedis.subscribe(evDefSub, evDefChannel);
    }

}
