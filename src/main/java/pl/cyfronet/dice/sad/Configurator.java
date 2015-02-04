package pl.cyfronet.dice.sad;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by tomek on 04.02.15.
 */
public class Configurator {

    public static final Configurator INSTANCE = new Configurator();

    private Properties props;

    private Configurator() {
        props = new Properties();
        InputStream is = getClass().getClassLoader().getResourceAsStream("sad.properties");

        if (is != null) {
            try {
                props.load(is);
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            // throw exc
        }
    }

    public String getProperty(String propName) {
        return props.getProperty(propName);
    }
}
