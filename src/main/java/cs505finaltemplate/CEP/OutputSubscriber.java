package cs505finaltemplate.CEP;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import cs505finaltemplate.Launcher;
import io.siddhi.core.util.transport.InMemoryBroker;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

public class OutputSubscriber implements InMemoryBroker.Subscriber {

    private String topic;

    final Type mapType = new TypeToken<Map<String, Integer>>() {
    }.getType();
    private Gson gson = new Gson();

    public OutputSubscriber(String topic, String streamName) {
        this.topic = topic;
    }

    @Override
    public void onMessage(Object msg) {

        try {
            // {"event":{"zip_code":12345,"count":123}}
            String msgStr = msg.toString();

            // {"zip_code":12345,"count":123}
            msgStr = msgStr.substring(9, msgStr.length() - 1);

            // 2 rows in the map: zip code and count.
            Map<String, Integer> msgMap = gson.fromJson(msgStr, mapType);
            int zipCode = msgMap.get("zip_code");
            int currentCount = msgMap.get("count");

            if (!Launcher.lastCEPOutput.isEmpty() && Launcher.lastCEPOutput.containsKey(zipCode)) {
                int lastCount = Launcher.lastCEPOutput.get(zipCode);
                // 2x jump from last message
                if (currentCount >= lastCount * 2 && !Launcher.alert_list.contains(zipCode)) {
                    Launcher.alert_list.add(zipCode);
                } else if (currentCount < lastCount * 2 && Launcher.alert_list.contains(zipCode)) {
                    Launcher.alert_list.remove(zipCode);
                }
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    @Override
    public String getTopic() {
        return topic;
    }

}
