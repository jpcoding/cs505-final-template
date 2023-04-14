package cs505finaltemplate.CEP;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import cs505finaltemplate.Launcher;
import io.siddhi.core.util.transport.InMemoryBroker;

public class OutputSubscriber implements InMemoryBroker.Subscriber {

    private String topic;

    final Type typeOfListMap = new TypeToken<List<Map<String, String>>>() {
    }.getType();
    private Gson gson = new Gson();

    public OutputSubscriber(String topic, String streamName) {
        this.topic = topic;
    }

    @Override
    public void onMessage(Object msg) {

        try {
            System.out.println("OUTPUT CEP EVENT: " + msg + "\n");

            Map<String, String> msgMap = gson.fromJson(msg.toString(), typeOfListMap);
            // @todo: compare msg with lastCEPOutput either here or in CEPEngine - need to
            // change alert_list in Launcher, then output that in getzipalerts and getalerts
            // in API.java.

            for (Map.Entry<String, String> entry : msgMap.entrySet()) {
                String key = entry.getKey();
                int currentVal = Integer.parseInt(entry.getValue());

                if (Launcher.lastCEPOutput.containsKey(key)) {

                    int lastVal = Integer.parseInt(Launcher.lastCEPOutput.get(key));
                    // 2x jump from last message
                    if (currentVal >= lastVal * 2 && !Launcher.alert_list.contains(Integer.parseInt(key))) {
                        Launcher.alert_list.add(Integer.parseInt(key));

                    } else if (currentVal < lastVal * 2 && Launcher.alert_list.contains(Integer.parseInt(key))) {
                        Launcher.alert_list.remove(Integer.parseInt(key));

                    }
                }
            }

            Launcher.lastCEPOutput.clear();
            Launcher.lastCEPOutput.putAll(msgMap);

        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    @Override
    public String getTopic() {
        return topic;
    }

}
