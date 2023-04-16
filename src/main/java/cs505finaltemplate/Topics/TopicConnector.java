package cs505finaltemplate.Topics;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import cs505finaltemplate.Launcher;
import cs505finaltemplate.graphDB.GraphDBEngine;
import io.siddhi.query.api.expression.condition.In;

import java.lang.reflect.Type;
import java.security.acl.LastOwnerException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TopicConnector {

    private Gson gson;

    final Type typeOfListMap = new TypeToken<List<Map<String, String>>>() {
    }.getType();
    final Type typeListTestingData = new TypeToken<List<TestingData>>() {
    }.getType();

    final Type typeListHospitalData = new TypeToken<List<HospitalData>>() {
    }.getType();

    final Type typeListVaxData = new TypeToken<List<VaxData>>() {
    }.getType();

    // private String EXCHANGE_NAME = "patient_data";
    Map<String, String> config;

    public TopicConnector(Map<String, String> config) {
        gson = new Gson();
        this.config = config;
    }

    public void connect() {

        try {

            // create connection factory, this can be used to create many connections
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(config.get("hostname"));
            factory.setPort(Integer.parseInt(config.get("port")));
            factory.setUsername(config.get("username"));
            factory.setPassword(config.get("password"));
            factory.setVirtualHost(config.get("virtualhost"));

            // create a connection, many channels can be created from a single connection
            Connection connection = factory.newConnection();
            Channel channel = connection.createChannel();

            patientListChannel(channel);
             hospitalListChannel(channel);
             vaxListChannel(channel);

        } catch (Exception ex) {
            System.out.println("connect Error: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    private void patientListChannel(Channel channel) {
        try {

            System.out.println("Creating patient_list channel");

            String topicName = "patient_list";

            channel.exchangeDeclare(topicName, "topic");
            String queueName = channel.queueDeclare().getQueue();

            channel.queueBind(queueName, topicName, "#");

            System.out.println(" [*] Paitent List Waiting for messages. To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {

                String message = new String(delivery.getBody(), "UTF-8");
                List<TestingData> incomingList = gson.fromJson(message, typeListTestingData);

                Map<Integer, Integer> zipCount = new HashMap<>();

                for (TestingData testingData : incomingList) {
                    //Check data integrity
                    if(!testingData.isValid()) {
                        continue;
                    }
                    // Check if this data is perfect data first.
                    Gson patient_info = new Gson();
                    String patient_info_jsonstring = patient_info.toJson(testingData);
                    try {
//                        System.out.println(patient_info_jsonstring);
                        Launcher.graphDBEngine.addPatient(testingData);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }

                    // if status is 1 and zip already in map, increment. else add to map.
                    if (testingData.patient_status.equals("1")) {
                        int zipcode = Integer.parseInt(testingData.patient_zipcode);
                        if (zipCount.containsKey(zipcode)) {
                            zipCount.put(zipcode, zipCount.get(zipcode) + 1);
                        } else {
                            zipCount.put(zipcode, 1);
                        }
                    }
                }
                // feed positive zip code counts into output subscriber
                Gson gson = new Gson();
                Map<String, Integer> inputMap = new HashMap<>();

                for (Map.Entry<Integer, Integer> entry : zipCount.entrySet()) {
                    inputMap.put("zip_code", entry.getKey());
                    inputMap.put("count", entry.getValue());
                    String inputJson = gson.toJson(inputMap);

                    Launcher.cepEngine.input("testInStream", inputJson);
                }
                // update lastCEPOutput
                Launcher.lastCEPOutput.clear();
                Launcher.lastCEPOutput.putAll(zipCount);

                // @todo: delete debug lines when done
                System.out.println("Last CEP Output: " + Launcher.lastCEPOutput.toString());
                System.out.println("Alert list: " + Launcher.alert_list.toString());
                System.out.println("Statewide alert: " + ((Launcher.cepEngine.getStateAlert()) ? "ON" : "OFF"));
            };

            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
            });

        } catch (Exception ex) {
            System.out.println("patientListChannel Error: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    private void hospitalListChannel(Channel channel) {
        try {

            String topicName = "hospital_list";

            System.out.println("Creating hospital_list channel");

            channel.exchangeDeclare(topicName, "topic");
            String queueName = channel.queueDeclare().getQueue();

            channel.queueBind(queueName, topicName, "#");

            System.out.println(" [*] Hospital List Waiting for messages. To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {

                // new message
                String message = new String(delivery.getBody(), "UTF-8");

                // convert string to class
                List<HospitalData> incomingList = gson.fromJson(message, typeListHospitalData);
                for (HospitalData hospitalData : incomingList) {
                    if(!hospitalData.isValid()) {
                        continue;
                    }
                    try {
                        Launcher.graphDBEngine.addHospital(hospitalData);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            };

            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
            });

        } catch (Exception ex) {
            System.out.println("hospitalListChannel Error: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

    private void vaxListChannel(Channel channel) {
        try {

            String topicName = "vax_list";

            System.out.println("Creating vax_list channel");

            channel.exchangeDeclare(topicName, "topic");
            String queueName = channel.queueDeclare().getQueue();

            channel.queueBind(queueName, topicName, "#");

            System.out.println(" [*] Vax List Waiting for messages. To exit press CTRL+C");

            DeliverCallback deliverCallback = (consumerTag, delivery) -> {

                String message = new String(delivery.getBody(), "UTF-8");

                // convert string to class
                List<VaxData> incomingList = gson.fromJson(message, typeListVaxData);
                for (VaxData vaxData : incomingList) {
                    if(!vaxData.isValid()) {
                        continue;
                    }
                    try {
                        Launcher.graphDBEngine.addVaccine(vaxData);
                    } catch (Exception e) {
                        e.printStackTrace();
                    }
                }

            };

            channel.basicConsume(queueName, true, deliverCallback, consumerTag -> {
            });

        } catch (Exception ex) {
            System.out.println("vaxListChannel Error: " + ex.getMessage());
            ex.printStackTrace();
        }
    }

}
