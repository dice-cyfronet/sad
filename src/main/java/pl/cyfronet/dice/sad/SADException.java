package pl.cyfronet.dice.sad;

import java.net.URISyntaxException;

/**
 * Created by tomek on 8/3/15.
 */
public class SADException extends Exception {
    public SADException(String exMsg, Throwable cause) {
        super(exMsg, cause);
    }

    public SADException(String exMsg) { super(exMsg); }
}
