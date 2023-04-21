package cs505finaltemplate.CEP;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import cs505finaltemplate.Launcher;
import io.siddhi.core.util.transport.InMemoryBroker;

import java.lang.reflect.Type;
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
            Map<String, Integer> currentMap = gson.fromJson(msgStr, mapType);
            int currentZip = currentMap.get("zip_code");
            int currentCount = currentMap.get("count");

            if (!Launcher.lastCEPOutput.isEmpty() && Launcher.lastCEPOutput.containsKey(currentZip)) {
                int lastCount = Launcher.lastCEPOutput.get(currentZip);
                // 2x jump from last message
                if (currentCount >= lastCount * 2 && !Launcher.alert_list.contains(currentZip)) {
                    Launcher.alert_list.add(currentZip);
                } else if (currentCount < lastCount * 2 && Launcher.alert_list.contains(currentZip)) {
                    Launcher.alert_list.remove(Launcher.alert_list.indexOf(currentZip));
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
