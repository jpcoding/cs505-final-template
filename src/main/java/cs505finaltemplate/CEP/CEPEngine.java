package cs505finaltemplate.CEP;

import com.google.gson.Gson;
import io.siddhi.core.SiddhiAppRuntime;
import io.siddhi.core.SiddhiManager;
import io.siddhi.core.stream.output.sink.InMemorySink;
import io.siddhi.core.util.transport.InMemoryBroker;
import io.siddhi.query.api.definition.TableDefinition;

import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

public class CEPEngine {

    private SiddhiManager siddhiManager;
    private SiddhiAppRuntime siddhiAppRuntime;
    private Map<String, String> topicMap;

    // table definition queries
    private String zipAlertTable = "zipAlerts";
    private String lastAlertTable = "lastAlert";
    private String zipAlertQueryString = "@PrimaryKey('zip_code') " + "define table " + zipAlertTable
            + " (zip_code int, alert_status bool)";
    private String lastMessageTableQueryString = "define table " + lastAlertTable + " (zip_code int, count int)";

    private Gson gson;

    public CEPEngine() {

        Class JsonClassSource = null;
        Class JsonClassSink = null;

        try {
            JsonClassSource = Class.forName("io.siddhi.extension.map.json.sourcemapper.JsonSourceMapper");
            JsonClassSink = Class.forName("io.siddhi.extension.map.json.sinkmapper.JsonSinkMapper");
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        try {
            InMemorySink sink = new InMemorySink();
            sink.connect();
        } catch (Exception ex) {
            ex.printStackTrace();
        }

        topicMap = new ConcurrentHashMap<>();

        // Creating Siddhi Manager
        siddhiManager = new SiddhiManager();
        siddhiManager.setExtension("sourceMapper:json", JsonClassSource);
        siddhiManager.setExtension("sinkMapper:json", JsonClassSink);
        gson = new Gson();
    }

    public void createCEP(String inputStreamName, String outputStreamName, String inputStreamAttributesString,
            String outputStreamAttributesString, String queryString) {
        try {

            String inputTopic = UUID.randomUUID().toString();
            String outputTopic = UUID.randomUUID().toString();

            topicMap.put(inputStreamName, inputTopic);
            topicMap.put(outputStreamName, outputTopic);

            String sourceString = getSourceString(inputStreamAttributesString, inputTopic, inputStreamName);
            System.out.println("sourceString: [" + sourceString + "]");
            String sinkString = getSinkString(outputTopic, outputStreamName, outputStreamAttributesString);
            System.out.println("sinkString: [" + sinkString + "]");
            // Generating runtime

            siddhiAppRuntime = siddhiManager
                    .createSiddhiAppRuntime(sourceString + " " + sinkString + " " + queryString);

            InMemoryBroker.Subscriber subscriberTest = new OutputSubscriber(outputTopic, outputStreamName);

            // subscribe to "inMemory" broker per topic
            InMemoryBroker.subscribe(subscriberTest);

            // Starting event processing
            siddhiAppRuntime.start();

            initializeTables();

        } catch (Exception ex) {
            ex.printStackTrace();
        }

    }

    public void input(String streamName, String jsonPayload) {
        try {

            if (topicMap.containsKey(streamName)) {
                // InMemoryBroker.publish(topicMap.get(streamName),
                // getByteGenericDataRecordFromString(schemaMap.get(streamName),jsonPayload));
                InMemoryBroker.publish(topicMap.get(streamName), jsonPayload);

            } else {
                System.out.println("input error : no schema");
            }

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void cleanDB() {
        try {
            // clean cep database
            String query = "delete from " + zipAlertTable;
            siddhiAppRuntime.query(query);

        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    public void getZipAlerts() {

    }

    public void getStateAlert() {

    }

    // create tables for the cep to remember stuff
    private void initializeTables() {
        siddhiAppRuntime.query(zipAlertQueryString);
        siddhiAppRuntime.query(lastMessageTableQueryString);
        // @todo: get rid of debug lines
        System.out.println("table definition:\n");
        Map<String, TableDefinition> tableDef = siddhiAppRuntime.getTableDefinitionMap();
        for (Map.Entry<String, TableDefinition> entry : tableDef.entrySet()) {
            System.out.println("\t" + entry.getKey() + ": " + entry.getValue() + "\n");
        }
    }

    private String getSourceString(String inputStreamAttributesString, String topic, String streamName) {
        String sourceString = null;
        try {

            sourceString = "@source(type='inMemory', topic='" + topic + "', @map(type='json')) " + "define stream "
                    + streamName + " (" + inputStreamAttributesString + "); ";

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return sourceString;
    }

    private String getSinkString(String topic, String streamName, String outputSchemaString) {
        String sinkString = null;
        try {

            sinkString = "@sink(type='inMemory', topic='" + topic + "', @map(type='json')) " + "define stream "
                    + streamName + " (" + outputSchemaString + "); ";

        } catch (Exception ex) {
            ex.printStackTrace();
        }

        return sinkString;
    }

}
