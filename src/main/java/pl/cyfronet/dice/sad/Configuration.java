package pl.cyfronet.dice.sad;

import java.util.Properties;

/**
 * Created by tomek on 8/18/15.
 */
public class Configuration {
    private String eventDefinitionsChannelName;
    private String simpleEventsChannelName;
    private String redisHost;
    private int redisPort;
    private int redisTimeout;
    private String redisPassword;

    public static Configuration fromPropertiesFile() {
        Properties props = Configurator.INSTANCE.getProperties();
        return new Configuration(
                props.getProperty("eventDefinitionChannel"),
                props.getProperty("simpleEventChannel"),
                props.getProperty("redisHost"),
                Integer.parseInt(props.getProperty("redisPort")),
                Integer.parseInt(props.getProperty("redisTimeout")),
                props.getProperty("redisPassword")
        );
    }

    public String getEventDefinitionsChannelName() {
        return eventDefinitionsChannelName;
    }

    public String getSimpleEventsChannelName() {
        return simpleEventsChannelName;
    }

    public String getRedisHost() {
        return redisHost;
    }

    public int getRedisPort() {
        return redisPort;
    }

    public int getRedisTimeout() {
        return redisTimeout;
    }

    public String getRedisPassword() {
        return redisPassword;
    }

    private Configuration(
            String eventDefinitionsChannelName,
            String simpleEventsChannelName,
            String redisHost,
            int redisPort,
            int redisTimeout,
            String redisPassword
    ) {
        this.eventDefinitionsChannelName = eventDefinitionsChannelName;
        this.simpleEventsChannelName = simpleEventsChannelName;
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.redisTimeout = redisTimeout;
        this.redisPassword = redisPassword;
    }
}