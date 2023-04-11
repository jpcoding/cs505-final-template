package cs505finaltemplate;

import cs505finaltemplate.CEP.CEPEngine;
import cs505finaltemplate.Topics.TopicConnector;
import cs505finaltemplate.graphDB.GraphDBEngine;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;

public class Launcher {

    public static GraphDBEngine graphDBEngine;
    public static String inputStreamName;
    public static CEPEngine cepEngine;
    public static TopicConnector topicConnector;
    public static final int WEB_PORT = 8082;

    public static String lastCEPOutput = "{}";

    public static void main(String[] args) throws IOException {

        // graphDBEngine = new GraphDBEngine();

        // **** start CEP init

        cepEngine = new CEPEngine();

        System.out.println("Starting CEP...");

        inputStreamName = "testInStream";
        String inputStreamAttributesString = "zip_code string";

        String outputStreamName = "testOutStream";
        String outputStreamAttributesString = "zip_code string, count long";

        // positive case counts per zip_code, grouped by zip_code
        // @todo: still need to figure out how to compare two message batches.
        // maybe do that in each channel? but then how do we keep track of alert zips
        // and non-alert zips? Maybe an RDB with like one or two tables, if the zip is
        // in the table then its in alert status. if the table has 5+ rows then the
        // state is in alert status.

        // table 1: zip_code int, alert_status boolean. initialize table with every zip
        // in KY at alert_status False.
        // you can use siddhi for the tables.
        System.out.println("boi\n");
        String alertQueryString = "from " + inputStreamName + "#window.timeBatch(5 sec) "
                + "select zip_code, count() as count " + "having patient_status == 1 " + // 1 is positive, 0 is negative
                "group by zip_code " + "insert into testOutStream; ";

        cepEngine.createCEP(inputStreamName, outputStreamName, inputStreamAttributesString,
                outputStreamAttributesString, alertQueryString);

        // **** end CEP Init

        // start message collector
        Map<String, String> message_config = new HashMap<>();
        message_config.put("hostname", "vbu231.cs.uky.edu");
        message_config.put("port", "9099"); //
        message_config.put("username", "team_8");
        message_config.put("password", "myPassCS505");
        message_config.put("virtualhost", "8");

        topicConnector = new TopicConnector(message_config);

        topicConnector.connect();
        System.out.println("Starting Web Server...");

        // end message collector

        // Embedded HTTP initialization
        startServer();

        try {
            while (true) {
                Thread.sleep(5000);
                System.out.println("sleeping...");
            }
        } catch (Exception ex) {
            ex.printStackTrace();
        }
    }

    private static void startServer() throws IOException {

        final ResourceConfig rc = new ResourceConfig().packages("cs505finaltemplate.httpcontrollers");

        System.out.println("Starting Web Server...");
        URI BASE_URI = UriBuilder.fromUri("http://0.0.0.0/").port(WEB_PORT).build();
        HttpServer httpServer = GrizzlyHttpServerFactory.createHttpServer(BASE_URI, rc);

        try {
            httpServer.start();
            System.out.println("Web Server Started...");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
