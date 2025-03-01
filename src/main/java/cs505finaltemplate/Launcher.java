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
import java.util.List;
import java.util.ArrayList;

public class Launcher {

    public static GraphDBEngine graphDBEngine;
    public static String inputStreamName;
    public static CEPEngine cepEngine;
    public static TopicConnector topicConnector;
    public static final int WEB_PORT = 9999;

    public static Map<Integer, Integer> lastCEPOutput = new HashMap<>();
    public static List<Integer> alert_list = new ArrayList<Integer>();

    public static void main(String[] args) throws IOException {

        graphDBEngine = new GraphDBEngine("test");
        // **** start CEP init

        cepEngine = new CEPEngine();

        System.out.println("Starting CEP...");

        inputStreamName = "testInStream";
        String inputStreamAttributesString = "zip_code long, count long";

        String outputStreamName = "testOutStream";
        String outputStreamAttributesString = "zip_code long, count long";

        // positive case counts per zip_code, grouped by zip_code
        String alertQueryString = "from testInStream " + "select zip_code, count " + "insert into testOutStream;";

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
        // message_config.put("username", "guest");
        // message_config.put("password", "guest");
        // message_config.put("virtualhost", "1");
        // message_config.put("hostname", "192.168.64.12");
        // message_config.put("port", "5672");
        topicConnector = new TopicConnector(message_config);

        topicConnector.connect();
        System.out.println("Starting Web Server json...");

        // end message collector

        // Embedded HTTP initialization
        startServer();

        try {
            while (true) {
                Thread.sleep(5000);
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
