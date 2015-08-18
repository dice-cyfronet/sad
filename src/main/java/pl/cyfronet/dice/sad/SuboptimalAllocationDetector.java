package pl.cyfronet.dice.sad;

import com.espertech.esper.client.UpdateListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import pl.cyfronet.dice.sad.cep.*;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisPubSub;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Phaser;

/**
 * Created by tomek on 04.02.15.
 */
public class SuboptimalAllocationDetector {

    private static final Logger log = LoggerFactory.getLogger(SuboptimalAllocationDetector.class);

    private final Phaser phaser = new Phaser();
    private final Configuration config;
    private JedisPool jedisPool;
    private UpdateListener complexEvListener;
    private EventDefinitionsManager evDefMng;
    private JedisPubSub evDefSub;
    private SimpleEventSub simpleEventSub;

    public SuboptimalAllocationDetector() throws SADException {
        config = Configuration.fromPropertiesFile();
        evDefSub = EventDefinitionsSub.getInstance();
        simpleEventSub = new SimpleEventSub();
        complexEvListener = RedisComplexEventSink.getInstance();
    }

    public static void main(String[] args) {
        log.info("Starting Suboptimal Allocation Detector");
        SuboptimalAllocationDetector sad = null;
        try {
            sad = new SuboptimalAllocationDetector();
        } catch (SADException se) {
            log.error(
                    "Failed to create SAD application:" + se.getMessage()
            );
            System.exit(1);
        }
        sad.start();
    }

    private void start() {
        setupJedisPool();
        setupEvDefinitionsListener();
        setupSimpleEvListener();
    }

    private void setupJedisPool() {
        jedisPool = new JedisPool(
                new JedisPoolConfig(),
                config.getRedisHost(),
                config.getRedisPort(),
                config.getRedisTimeout(),
                config.getRedisPassword()
        );
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

    private void setupEvDefinitionsListener() {
        new Thread(() -> {
            phaser.register();
            Jedis jedis = jedisPool.getResource();
            jedis.subscribe(evDefSub, config.getEventDefinitionsChannelName());
            log.debug("Releasing Jedis resource from EventDefinitionListener");
            jedisPool.returnResource(jedis);
            phaser.arrive();
        }).start();
        log.info("Subscribed for event definitions");
    }

    private void setupSimpleEvListener() {
        new Thread(() -> {
            phaser.register();
            Jedis jedis = jedisPool.getResource();
            jedis.subscribe(simpleEventSub, config.getEventDefinitionsChannelName());
            log.debug("Releasing Jedis resource from SimpleEventListener");
            jedisPool.returnResource(jedis);
            phaser.arrive();
        }).start();
        log.info("Subscribed for simple events");
    }

    //========================================================================================

    private static void testLoadEvent() throws InterruptedException, URISyntaxException, SADException {
        Engine engine = Engine.getInstnace();
        Map<String, Object> eventDef = new HashMap<String, Object>();
        eventDef.put("vmUuid", String.class);
        eventDef.put("cpuLoad", float.class);
        engine.addEventType("CpuLoad1", eventDef);
        UpdateListener listener = RedisComplexEventSink.getInstance();
        String complexEvDef = "select avg(cpuLoad), vmUuid from CpuLoad1.win:time(5 sec)"
                            + " having avg(cpuLoad) > 0.8 output first every 10 seconds";
        engine.subscribe(complexEvDef, listener);
        Map eventMap;
        for(int i=0; i < 20; i++) {
            eventMap = new HashMap();
            eventMap.put("cpuLoad", i * 0.2);
            eventMap.put("vmUuid", "1");
            engine.sendEvent(eventMap, "CpuLoad1");
            log.info("Sent event for 1 " + i * 0.2);
            log.info("--------------------");
            Thread.sleep(900);
        }
        engine.unsubscribe(complexEvDef);
        engine.removeEventType("CpuLoad1");
        log.info("Finished!");

    }

}
