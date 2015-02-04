package pl.cyfronet.dice.sad.cep;

import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.UpdateListener;

/**
* Created by tomek on 04.02.15.
*/
public class Subscription {
    private EPStatement statement;
    private UpdateListener listener;

    public Subscription(EPStatement statement, UpdateListener listener) {
        this.statement = statement;
        this.listener = listener;
    }

    public EPStatement getStatement() {
        return statement;
    }

    public UpdateListener getListener() {
        return listener;
    }
}
