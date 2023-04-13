package cs505finaltemplate;
import com.google.gson.reflect.TypeToken;
import cs505finaltemplate.CEP.CEPEngine;
import cs505finaltemplate.Topics.TestingData;
import cs505finaltemplate.Topics.HospitalData;
import cs505finaltemplate.Topics.VaxData;
import cs505finaltemplate.Topics.TopicConnector;
import cs505finaltemplate.graphDB.GraphDBEngine;
import org.glassfish.grizzly.http.server.HttpServer;
import org.glassfish.jersey.grizzly2.httpserver.GrizzlyHttpServerFactory;
import org.glassfish.jersey.server.ResourceConfig;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.io.FileReader;
import com.google.gson.stream.JsonReader;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;




public class TestGraph
{
    public static void main(String[] args)throws IOException
    {
        final Type typeOfListMap = new TypeToken<List<Map<String,String>>>(){}.getType();
        final Type typeListTestingData = new TypeToken<List<TestingData>>(){}.getType();
        final Type typeListHospitalData = new TypeToken<List<HospitalData>>(){}.getType();
        final Type typeListVaxData = new TypeToken<List<VaxData>>(){}.getType();

        Gson gson = new Gson();
        System.out.println("Testing Graph");
        GraphDBEngine graphDBEngine = new GraphDBEngine("test");
//
//        try {
//            // Open the JSON file for reading
//            FileReader reader = new FileReader("rPublisher-master/patient_data.json");
//            // Use TypeToken to specify the type of the list
//            // Deserialize the JSON data into a list of objects
//            List<TestingData> data = gson.fromJson(reader, typeListTestingData);
//            // Close the file
//            reader.close();
//            // Iterate over the list and print each object
//            for (TestingData testingData : data) {
////                System.out.println(testingData.patient_name);
////                System.out.println(testingData.patient_mrn);
//                Gson patient_info = new Gson();
//                String patient_info_jsonstring = patient_info.toJson(testingData);
//                graphDBEngine.addPatient(patient_info_jsonstring);
//            }
//
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
        // create a graph database
        // add the data to the graph database
        // query the graph database
        // close the graph database
//        graphDBEngine.getContacts("1def254c-b4e8-11ec-a016-ac87a3187c5f");
        // add hospital data
        try {
            // Open the JSON file for reading
            FileReader reader = new FileReader("rPublisher-master/hospital_data.json");
            List<HospitalData> data = gson.fromJson(reader, typeListHospitalData);
            reader.close();
            for (HospitalData testingData : data) {
                System.out.println(testingData.patient_name);
                graphDBEngine.addHospital(testingData);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

//         add vax data
        try{
            FileReader reader = new FileReader("rPublisher-master/vax_data.json");
            List<VaxData> data = gson.fromJson(reader, typeListVaxData);
            reader.close();
            for (VaxData testingData : data) {
                System.out.println(testingData.vaccination_id);
                graphDBEngine.addVaccine(testingData);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        graphDBEngine.closeBD();
//

    }
}
