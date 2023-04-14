package cs505finaltemplate.graphDB;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import cs505finaltemplate.Topics.HospitalData;
import cs505finaltemplate.Topics.TestingData;
import cs505finaltemplate.Topics.VaxData;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.db.ODatabaseType;

import java.util.List;
import java.util.Map;
import java.util.HashMap;
import java.util.ArrayList;

import com.google.gson.Gson;


public class GraphDBEngine {

    private String dbName;

    //!!! CODE HERE IS FOR EXAMPLE ONLY, YOU MUST CHECK AND MODIFY!!!
    public GraphDBEngine(String dbName) {
        OrientDB orient;
        ODatabaseSession db;
        this.dbName = dbName;

        orient = new OrientDB("remote:pji228.cs.uky.edu", "root", "rootpwd", OrientDBConfig.defaultConfig());
        resetDB(orient, dbName);
        db = orient.open(dbName, "root", "rootpwd");
        // create schema
        // patient node and edge
        OClass patient = db.getClass("patient");
        if (patient == null) {
            patient = db.createVertexClass("patient");
        }

        if (patient.getProperty("patient_mrn") == null) {
            patient.createProperty("patient_mrn", OType.STRING);
            patient.createIndex("patient_name_index", OClass.INDEX_TYPE.NOTUNIQUE, "patient_mrn");
        }

        if (db.getClass("contact_with") == null) {
            db.createEdgeClass("contact_with");
        }
        // hospital node and edge
        OClass hospital = db.getClass("hospital");
        if (hospital == null) {
            hospital = db.createVertexClass("hospital");
        }
        if (hospital.getProperty("hospital_id") == null) {
            hospital.createProperty("hospital_id", OType.STRING);
            hospital.createIndex("hospital_id_index", OClass.INDEX_TYPE.NOTUNIQUE, "hospital_id");
        }
        if (db.getClass("hospitalized_at") == null) {
            db.createEdgeClass("hospitalized_at");
        }
        // event node and edge
        OClass event = db.getClass("event");
        if (event == null) {
            event = db.createVertexClass("event");
        }
        if (event.getProperty("event_id") == null) {
            event.createProperty("event_id", OType.STRING);
            event.createIndex("event_id_index", OClass.INDEX_TYPE.NOTUNIQUE, "event_id");
        }
        if (db.getClass("attended") == null) {
            db.createEdgeClass("attended");
        }
        // vaccine node and edge
        OClass vaccine = db.getClass("vaccine");
        if (vaccine == null) {
            vaccine = db.createVertexClass("vaccine");
        }
        if (vaccine.getProperty("vaccine_id") == null) {
            vaccine.createProperty("vaccine_id", OType.STRING);
            vaccine.createIndex("vaccine_id_index", OClass.INDEX_TYPE.NOTUNIQUE, "vaccine_id");
        }
        if (db.getClass("vaccinated_with") == null) {
            db.createEdgeClass("vaccinated_with");
        }


        db.close();
//        orient.close();

    }

    public void resetDB(OrientDB db, String dbName) {
        if (db.exists(dbName)) {
            db.drop(dbName);
        }
        db.create(dbName, ODatabaseType.PLOCAL);
    }

    public void resetDB() {
        OrientDB orient;
        orient = new OrientDB("remote:pji228.cs.uky.edu", "root", "rootpwd", OrientDBConfig.defaultConfig());
        resetDB(orient, dbName);
    }

//    public void closeBD() {
//
//        orient.close();
//    }

    public void cleadData() {
        OrientDB orient;
        ODatabaseSession db;
        orient = new OrientDB("remote:pji228.cs.uky.edu", "root", "rootpwd", OrientDBConfig.defaultConfig());
        db = orient.open(dbName, "root", "rootpwd");
        db.command("DELETE VERTEX patient");
        db.command("DELETE VERTEX hospital");
        db.command("DELETE VERTEX event");
        db.command("DELETE VERTEX vaccine");
        db.close();
        orient.close();

    }

    private OVertex createPatient(ODatabaseSession db, String patient_mrn) {
        OVertex result = db.newVertex("patient");
        result.setProperty("patient_mrn", patient_mrn);
        result.save();
        return result;
    }

    private void addPatient(TestingData patient) {
        // If the node already exists, update it
        // If the node does not exist, create it
        // If the node has a contact, create an edge between the two nodes
        OrientDB orient;
        ODatabaseSession db;
        orient = new OrientDB("remote:pji228.cs.uky.edu", "root", "rootpwd", OrientDBConfig.defaultConfig());
        db = orient.open(dbName, "root", "rootpwd");
        String query = "SELECT FROM patient WHERE patient_mrn = ?";
        OResultSet rs = db.query(query, patient.patient_mrn);
//        if (!rs.hasNext()) {
//            System.out.println("rs is null");
//        }
        if (rs.hasNext()) {
            OResult item = rs.next();
            if (item.isVertex()) {
                OVertex existingPatient = item.getVertex().get();
                existingPatient.setProperty("testing_id", patient.testing_id);
                existingPatient.setProperty("patient_mrn", patient.patient_mrn);
                existingPatient.setProperty("patient_name", patient.patient_name);
                existingPatient.setProperty("patient_status", patient.patient_status);
                existingPatient.setProperty("patient_zipcode", patient.patient_zipcode);
                existingPatient.setProperty("patient_status", patient.patient_status);
                existingPatient.setProperty("contact_list", patient.contact_list);
                existingPatient.setProperty("event_list", patient.event_list);
                // add event edges and nodes
                addEvent(patient, existingPatient, db);
                // add contacts
                addContacts(patient, existingPatient, db);
                existingPatient.save();
            }
        } else {
//            System.out.println("Creating new patient");
            OVertex newPatient = db.newVertex("patient");
            newPatient.setProperty("testing_id", patient.testing_id);
            newPatient.setProperty("patient_mrn", patient.patient_mrn);
            newPatient.setProperty("patient_name", patient.patient_name);
            newPatient.setProperty("patient_status", patient.patient_status);
            newPatient.setProperty("patient_zipcode", patient.patient_zipcode);
            newPatient.setProperty("patient_status", patient.patient_status);
            newPatient.setProperty("contact_list", patient.contact_list);
            newPatient.setProperty("event_list", patient.event_list);
            newPatient.save();
            // add event edges and nodes
            addEvent(patient, newPatient, db);
            // add contacts
            addContacts(patient, newPatient, db);
            newPatient.save();
        }
        rs.close();
        db.close();
        orient.close();
    }

    public void addPatient(int testing_id, String patient_name, String patient_mrn, String patient_zipcode, String patient_status, List<String> contact_list, List<String> event_list) {
        TestingData patient = new TestingData(testing_id, patient_mrn, patient_name, patient_status, patient_zipcode, contact_list, event_list);
        addPatient(patient);
    }

    public void addPatient(String jsonString) {
        Gson gson = new Gson();
//        System.out.println("jsonString: " + jsonString);
        TestingData patient = gson.fromJson(jsonString, TestingData.class);
//        System.out.println("testing_id: " + patient.testing_id);
//        System.out.println("patient_name: " + patient.patient_name);
//        System.out.println("patient_mrn: " + patient.patient_mrn);
//        System.out.println("patient_zipcode: " + patient.patient_zipcode);
//        System.out.println("patient_status: " + patient.patient_status);
        addPatient(patient);
    }


    public void addHospital(HospitalData hospital)
    {
        OrientDB orient;
        ODatabaseSession db;
        orient = new OrientDB("remote:pji228.cs.uky.edu", "root", "rootpwd", OrientDBConfig.defaultConfig());
        db = orient.open(dbName, "root", "rootpwd");

        String hospitalQuery = "SELECT FROM hospital WHERE hospital_id = ?";
        OResultSet hospitalRS = db.query(hospitalQuery, hospital.hospital_id);
//        System.out.println("hospitalQuery: " + hospitalQuery);
        if (hospitalRS.hasNext()) {
            OResult existingHospital = hospitalRS.next();
            if (existingHospital.isVertex()) {
                OVertex thisHospital = existingHospital.getVertex().get();
                thisHospital.setProperty("hospital_id", hospital.hospital_id);
                thisHospital.save();
                String patientQuery = "SELECT FROM patient WHERE patient_mrn = ?";
                OResultSet patientRS = db.query(patientQuery, hospital.patient_mrn);
                if (patientRS.hasNext()) {
                    existingHospital = patientRS.next();
                    if (existingHospital.isVertex()) {
                        OVertex thisPatient = existingHospital.getVertex().get();
                        // If there is no edge between the patient and the hospital, create one
                        String edgeQuery = "SELECT FROM hospitalized_at WHERE out = ? AND in = ?";
                        OResultSet edgeRS = db.query(edgeQuery, thisPatient, thisHospital);
                        if (!edgeRS.hasNext()) {
                            OEdge edge = thisPatient.addEdge(thisHospital, "hospitalized_at");
                            edge.save();
                        }
                        edgeRS.close();
                    }
                } else {
                    OVertex newPatient = db.newVertex("patient");
                    newPatient.setProperty("patient_mrn", hospital.patient_mrn);
                    newPatient.setProperty("patient_name", hospital.patient_name);
                    newPatient.save();
                    OEdge edge = newPatient.addEdge(thisHospital, "hospitalized_at");
                    edge.save();
                }
            }
        }
        else {
            // create hospital
            // if the patient exists, create edge
            // if the patient does not exist, create patient and edge
//            System.out.println("Creating new hospital");
            OVertex newHospital = db.newVertex("hospital");
            newHospital.setProperty("hospital_id", hospital.hospital_id);
            String patientQuery = "SELECT FROM patient WHERE patient_mrn = ?";
            OResultSet patientRS = db.query(patientQuery, hospital.patient_mrn);
            if (patientRS.hasNext()) {
                OResult patient = patientRS.next();
                if (patient.isVertex()) {
                    OVertex thisPatient = patient.getVertex().get();
                    // if there is no edge between the patient and the hospital, create one
                    String edgeQuery = "SELECT FROM hospitalized_at WHERE out = ? AND in = ?";
                    OResultSet edgeRS = db.query(edgeQuery, thisPatient, newHospital);
                    if (!edgeRS.hasNext()) {
                        OEdge edge = thisPatient.addEdge(newHospital, "hospitalized_at");
                        edge.save();
                    }
                    edgeRS.close();
                }
            } else {
                OVertex newPatient = db.newVertex("patient");
                newPatient.setProperty("patient_mrn", hospital.patient_mrn);
                newPatient.setProperty("patient_name", hospital.patient_name);
                newPatient.save();
                // create edge
                // If there is no edge between the patient and the hospital, create one
                String edgeQuery = "SELECT FROM hospitalized_at WHERE out = ? AND in = ?";
                OResultSet edgeRS = db.query(edgeQuery, newPatient, newHospital);
                if (!edgeRS.hasNext()) {
                    OEdge edge = newPatient.addEdge(newHospital, "hospitalized_at");
                    edge.save();
                }
                edgeRS.close();
            }
            newHospital.save();
        }
        hospitalRS.close();
        db.close();
        orient.close();
    }

    public void addVaccine(VaxData vaccine)
        {
            OrientDB orient;
            ODatabaseSession db;
            orient = new OrientDB("remote:pji228.cs.uky.edu", "root", "rootpwd", OrientDBConfig.defaultConfig());
            db = orient.open(dbName, "root", "rootpwd");
            String query = "SELECT FROM vaccine WHERE  vaccination_id = ?";
            OResultSet rs = db.query(query, vaccine.vaccination_id);
            if (rs.hasNext()) {
                OResult item = rs.next();
                if (item.isVertex()) {
                    // update vaccine
                    OVertex thisVaccine = item.getVertex().get();
                    String patientQuery = "SELECT FROM patient WHERE patient_mrn = ?";
                    rs = db.query(patientQuery, vaccine.patient_mrn);
                    if (rs.hasNext()) {
                        item = rs.next();
                        if (item.isVertex()) {
                            OVertex thisPatient = item.getVertex().get();
                            OEdge edge = thisPatient.addEdge(thisVaccine, "vaccinated_with");
                            edge.save();
                        }
                    } else {
                        OVertex newPatient = db.newVertex("patient");
                        newPatient.setProperty("patient_mrn", vaccine.patient_mrn);
                        newPatient.save();
                        OEdge edge = newPatient.addEdge(thisVaccine, "vaccinated_with");
                        edge.save();
                    }
                }
            }
            else {
                // the vaccine does not exist
                // create the vaccine
                // if the patient exists, create edge
                // if the patient does not exist, create patient and edge
                OVertex newVaccine = db.newVertex("vaccine");
                newVaccine.setProperty("vaccination_id", vaccine.vaccination_id);
                newVaccine.save();
                String patientQuery = "SELECT FROM patient WHERE patient_mrn = ?";
                rs = db.query(patientQuery, vaccine.patient_mrn);
                if (rs.hasNext()) {
                    OResult item = rs.next();
                    if (item.isVertex()) {
                        OVertex thisPatient = item.getVertex().get();
                        OEdge edge = thisPatient.addEdge(newVaccine, "vaccinated_with");
                        edge.save();
                    }
                } else {
                    OVertex newPatient = db.newVertex("patient");
                    newPatient.setProperty("patient_mrn", vaccine.patient_mrn);
                    newPatient.setProperty("patient_name", vaccine.patient_name);
                    newPatient.save();
                    OEdge edge = newPatient.addEdge(newVaccine, "vaccinated_with");
                    edge.save();
                }
            }
            rs.close();
            db.close();
            orient.close();
    }


    // Only used in the addPatient method
    private void addEvent(TestingData patient, OVertex existingPatient, ODatabaseSession db)
    {

        for (String event : patient.event_list) {
            String query1 = "SELECT FROM event WHERE event_id = ?";
            OResultSet rs1 = db.query(query1, event);
            if (rs1.hasNext()) {
                OResult item1 = rs1.next();
                if (item1.isVertex()) {
                    OVertex v = item1.getVertex().orElse(null);
                    // If node exists, create an edge between the two nodes
                    String edgeQuery = "SELECT FROM attended WHERE out = ? AND in = ?";
                    OResultSet edgeRS = db.query(edgeQuery, existingPatient, v);
                    if (!edgeRS.hasNext()) {
                        OEdge edge = existingPatient.addEdge(v, "attended");
                        edge.save();
                    }
                    edgeRS.close();
                }
            } else {
                // If node does not exist, create it and create an edge between the two nodes
                OVertex newEvent = db.newVertex("event");
                newEvent.setProperty("event_id", event);
                newEvent.save();
                // If edge does not exist, create it
                String edgeQuery = "SELECT FROM attended WHERE out = ? AND in = ?";
                OResultSet edgeRS = db.query(edgeQuery, existingPatient, newEvent);
                if (!edgeRS.hasNext()) {
                    OEdge edge = existingPatient.addEdge(newEvent, "attended");
                    edge.save();
                }
                edgeRS.close();
            }
            rs1.close();
        }
    }

    // Only used in the addPatient method
    private void addContacts(TestingData patient, OVertex existingPatient, ODatabaseSession db)
    {
        for (String contact : patient.contact_list) {
            if (!contact.equals(patient.patient_mrn)) {
                String contactsQuery = "SELECT FROM patient WHERE patient_mrn = ?";
                OResultSet contactsRS = db.query(contactsQuery, contact);
                if (contactsRS.hasNext()) {
                    OResult item = contactsRS.next();
                    if (item.isVertex()) {
                        OVertex v = item.getVertex().orElse(null);
                        OEdge edge = existingPatient.addEdge(v, "contact_with");
                        edge.save();
                    }
                } else {
                    OVertex newContact = db.newVertex("patient");
                    newContact.setProperty("patient_mrn", contact);
                    newContact.save();
                    OEdge edge = existingPatient.addEdge(newContact, "contact_with");
                    edge.save();
                }
                contactsRS.close();
            }
        }
    }
    public String getConfirmedContacts(String patient_mrn) {
        OrientDB orient;
        ODatabaseSession db;
        orient = new OrientDB("remote:pji228.cs.uky.edu", "root", "rootpwd", OrientDBConfig.defaultConfig());
        db = orient.open(dbName, "root", "rootpwd");
        System.out.println("patient_mrn = " + patient_mrn);
        String query = "SELECT FROM (TRAVERSE inE(), outE(), inV(), outV() \n" +
                "FROM (SELECT FROM patient WHERE patient_mrn = ?) \n" +
                "WHILE $depth <= 2)\n" +
                "WHERE @class = 'patient'\n";
//        query = "SELECT FROM (TRAVERSE in(), out() \n" +
//                "FROM (SELECT FROM patient WHERE patient_mrn = ?) \n" +
//                "WHILE $depth <= 2)\n" +
//                "WHERE @class = 'patient' AND @rid <> (SELECT FROM patient WHERE patient_mrn = ?).@rid\n";
        OResultSet rs = db.query(query, patient_mrn);
        List<String> contacts = new ArrayList<>();
        while (rs.hasNext()) {
            OResult item = rs.next();
//            if (!item.getProperty("patient_mrn").equals(patient_mrn)) {
                contacts.add(item.getProperty("patient_mrn"));
                System.out.println("contact: " + item.getProperty("patient_mrn"));
//            }
        }
        // create a hashmap to store the contact list
        HashMap<String, List<String>> contactList = new HashMap<>();
        contactList.put("contactlist", contacts);
        // convert the hashmap to json
        Gson gson = new Gson();
        String jsonString = gson.toJson(contactList);
        System.out.println(jsonString);
        rs.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
        db.close();
        orient.close();
        return jsonString;
    }

    public void getPossibleContacts(String patient_mrn) {

        String query = "SELECT FROM (TRAVERSE inE(), outE(), inV(), outV() \n" +
                "FROM (SELECT FROM patient WHERE patient_mrn = ?) \n" +
                "WHILE $depth <= 2)\n" +
                "WHERE @class = 'patient'\n";


    }

}




