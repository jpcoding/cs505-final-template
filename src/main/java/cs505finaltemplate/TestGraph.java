package cs505finaltemplate;
import cs505finaltemplate.CEP.CEPEngine;
import cs505finaltemplate.Topics.TestingData;
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
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;



public class TestGraph
{
    public static void main(String[] args)
    {

        TestingData testPatient = new TestingData("12345",
                "John",
                "Doe",
                12345,
                1,
                Arrays.asList("string1", "string2", "string3"),
                Arrays.asList("event1", "event2", "event3"));
        System.out.println("Testing Graph\n");
        GraphDBEngine graphDBEngine = new GraphDBEngine("testOrient");
//        graphDBEngine.GraphDBEngine();
//        graphDBEngine.addPatient(testPatient);
//        graphDBEngine.addPatient(testPatient);
//




    }
}
