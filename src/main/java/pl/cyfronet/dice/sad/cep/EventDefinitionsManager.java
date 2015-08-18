package pl.cyfronet.dice.sad.cep;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.cyfronet.dice.sad.Configurator;
import pl.cyfronet.dice.sad.SADException;
import redis.clients.jedis.*;

import java.util.concurrent.Phaser;

/**
 * Created by tomek on 8/3/15.
 */
public class EventDefinitionsManager {

    private static final Logger log = LoggerFactory.getLogger(EventDefinitionsManager.class);

    private JedisPool jedisPool;
    private String evDefChannel;
    private String simpleEventChannel;
    private JedisPubSub evDefSub;
    private SimpleEventSub simpleEventSub;

    private final Phaser phaser = new Phaser();

    public EventDefinitionsManager() throws SADException {
        Configurator configurator = Configurator.INSTANCE;
        evDefChannel = configurator.getProperty("eventDefinitionChannel");
        evDefSub = EventDefinitionsSub.getInstance();
        simpleEventChannel = configurator.getProperty("simpleEventChannel");
        simpleEventSub = new SimpleEventSub();
    }

    public void start() {
        setupJedisPool();
        setupEvDefinitionsListener();
        setupSimpleEvListener();
    }

    private void setupSimpleEvListener() {
        new Thread(() -> {
            phaser.register();
            Jedis jedis = jedisPool.getResource();
            jedis.subscribe(simpleEventSub, simpleEventChannel);
            log.debug("Releasing Jedis resource from SimpleEventListener");
            jedisPool.returnResource(jedis);
            phaser.arrive();
        }).start();
        log.info("Subscribed for simple events");
    }

    private void setupEvDefinitionsListener() {
        new Thread(() -> {
            phaser.register();
            Jedis jedis = jedisPool.getResource();
            jedis.subscribe(evDefSub, evDefChannel);
            log.debug("Releasing Jedis resource from EventDefinitionListener");
            jedisPool.returnResource(jedis);
            phaser.arrive();
        }).start();
        log.info("Subscribed for event definitions");
    }

    private void setupJedisPool() {
        Configurator configurator = Configurator.INSTANCE;
        String redisHost = configurator.getProperty("redisHost");
        int redisPort = Integer.parseInt(configurator.getProperty("redisPort"));
        int redisTimeout = Integer.parseInt(configurator.getProperty("redisTimeout"));
        String redisPassword = configurator.getProperty("redisPassword");
        jedisPool = new JedisPool(new JedisPoolConfig(), redisHost, redisPort, redisTimeout, redisPassword);
        Runtime.getRuntime().addShutdownHook(
                new Thread(
                        () -> {
                            phaser.register();
                            evDefSub.unsubscribe();
                            simpleEventSub.unsubscribe();
                            phaser.arriveAndAwaitAdvance();
                            if (jedisPool != null) {
                                jedisPool.destroy();
                                log.debug("Destroyed Jedis pool");
                            }
                        }
                )
        );
    }

}
