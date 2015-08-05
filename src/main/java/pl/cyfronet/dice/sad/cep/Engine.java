package pl.cyfronet.dice.sad.cep;

import com.espertech.esper.client.*;

import java.util.HashMap;
import java.util.Map;

/**
* Created by tomek on 04.02.15.
*/
public class Engine {

    private EPServiceProvider epService;
    private EPRuntime epRuntime;
    private Map<String, Subscription> subscriptions;

    public Engine() {
        epService = EPServiceProviderManager.getDefaultProvider();
        epRuntime = epService.getEPRuntime();
        subscriptions = new HashMap<>();
    }

    public void addEventType(String evName, Map<String, Object> evDef) {
        epService.getEPAdministrator().getConfiguration().addEventType(evName, evDef);
    }

    public boolean removeEventType(String evName) {
        return epService.getEPAdministrator().getConfiguration().removeEventType(evName, true);
    }

    public void sendEvent(Map evData, String evName) {
        epRuntime.sendEvent(evData, evName);
    }

    public void subscribe(String complexEvDef, UpdateListener listener) {
        EPStatement statement = epService.getEPAdministrator().createEPL(complexEvDef);
        statement.addListener(listener);
        Subscription subscription = new Subscription(statement, listener);
        subscriptions.put(complexEvDef, subscription);
    }

    public void unsubscribe(String complexEvDef) {
        Subscription subscription = subscriptions.get(complexEvDef);
        subscription.getStatement().removeListener(subscription.getListener());
    }

}
