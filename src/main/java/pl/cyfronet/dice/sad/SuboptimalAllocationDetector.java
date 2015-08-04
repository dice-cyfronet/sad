package pl.cyfronet.dice.sad;

import com.espertech.esper.client.UpdateListener;
import pl.cyfronet.dice.sad.cep.Engine;
import pl.cyfronet.dice.sad.cep.EventDefinitionsManager;
import pl.cyfronet.dice.sad.cep.RedisComplexEventSink;

import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by tomek on 04.02.15.
 */
public class SuboptimalAllocationDetector {

    private UpdateListener complexEvListener;
    private Engine engine;
    private EventDefinitionsManager evDefMng;

    public SuboptimalAllocationDetector() {
        try {
            engine = new Engine();
            evDefMng = new EventDefinitionsManager(engine);
            complexEvListener = new RedisComplexEventSink();
        } catch (SADException e) {
            System.out.println(
                    "Failed to start Suboptimal Allocation Detector:\n\t"
                    + e.getMessage()
            );
        }

    }

    public static void main(String[] args) throws InterruptedException, URISyntaxException {
        System.out.println("Starting Suboptimal Allocation Detector");
        new SuboptimalAllocationDetector().start();
    }

    private void start() {
        evDefMng.start();

    }

    private static void testLoadEvent() throws InterruptedException, URISyntaxException, SADException {
        Engine engine = new Engine();
        Map<String, Object> eventDef = new HashMap<String, Object>();
        eventDef.put("vmUuid", String.class);
        eventDef.put("cpuLoad", float.class);
        engine.addEventType("CpuLoad1", eventDef);
        UpdateListener listener = new RedisComplexEventSink();
        String complexEvDef = "select avg(cpuLoad), vmUuid from CpuLoad1.win:time(5 sec) having avg(cpuLoad) > 0.8 output first every 10 seconds";
        engine.subscribe(complexEvDef, listener);
        Map eventMap;
        for(int i=0; i < 20; i++) {
            eventMap = new HashMap();
            eventMap.put("cpuLoad", i * 0.2);
            eventMap.put("vmUuid", "1");
            engine.sendEvent(eventMap, "CpuLoad1");
            System.out.println("Sent event for 1 " + i * 0.2);
            System.out.println("--------------------");
            Thread.sleep(900);
        }
        engine.unsubscribe(complexEvDef);
        engine.removeEventType("CpuLoad1");
        System.out.println("Finished!");

    }

}
