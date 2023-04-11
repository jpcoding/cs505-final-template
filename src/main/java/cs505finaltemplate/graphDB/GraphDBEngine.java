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
import com.google.gson.Gson;




public class GraphDBEngine {
    private OrientDB orient;
    private ODatabaseSession db;
    private String dbName;

    //!!! CODE HERE IS FOR EXAMPLE ONLY, YOU MUST CHECK AND MODIFY!!!
    public GraphDBEngine(String dbName) {

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
        if(hospital.getProperty("hospital_id") == null) {
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
        if(event.getProperty("event_id") == null) {
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
        if(vaccine.getProperty("vaccine_id") == null) {
            vaccine.createProperty("vaccine_id", OType.STRING);
            vaccine.createIndex("vaccine_id_index", OClass.INDEX_TYPE.NOTUNIQUE, "vaccine_id");
        }
        if (db.getClass("vaccinated_with") == null) {
            db.createEdgeClass("vaccinated_with");
        }


//        db.close();
//        orient.close();

    }

    public void resetDB(OrientDB db, String dbName) {
        if (db.exists(dbName)) {
            db.drop(dbName);
        }
        db.create(dbName, ODatabaseType.PLOCAL);
    }
    public void resetDB() {
        resetDB(orient, dbName);
    }

    public void closeBD(){
        db.close();
        orient.close();
    }

    public void cleadData(){
        db.command("DELETE VERTEX patient");
        db.command("DELETE VERTEX hospital");
        db.command("DELETE VERTEX event");
        db.command("DELETE VERTEX vaccine");
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
        db = orient.open(dbName, "root", "rootpwd");

        String query = "SELECT FROM patient WHERE patient_mrn = ?" ;
//        OResultSet rs;
//        rs = null;
//        try{
//            System.out.println("1234: ");
//            rs = db.command(query, "1def220e-b4e8-11ec-a016-ac87a3187c5f");
//            System.out.println("patient_mrn: " + patient.patient_mrn);
//        }
//        catch (Exception ex)
//        {System.out.println("exception: ");
//            ex.printStackTrace();
//        }
        OResultSet rs = db.query(query, patient.patient_mrn);

        if (!rs.hasNext()) {
            System.out.println("rs is null");
        }

        if (rs.hasNext()) {
            OResult item = rs.next();
            if (item.isVertex()) {
                OVertex result = item.getVertex().get();
                result.setProperty("testing_id", patient.testing_id);
                result.setProperty("patient_mrn", patient.patient_mrn);
                result.setProperty("patient_name", patient.patient_name);
                result.setProperty("patient_status", patient.patient_status);
                result.setProperty("patient_zipcode", patient.patient_zipcode);
                result.setProperty("patient_status", patient.patient_status);
                result.setProperty("contact_list", patient.contact_list);
                for(String event:patient.event_list)
                {
                    String query1 = "SELECT FROM event WHERE event_id = ?";
                    OResultSet rs1 = db.query(query1, event);
                    if (rs1.hasNext()) {
                        OResult item1 = rs1.next();
                        if (item1.isVertex()) {
                            OVertex  v = item1.getVertex().orElse(null);
                            OEdge edge = result.addEdge(v, "attended");
                            edge.save();
                        }
                    } else {
                        OVertex newEvent = db.newVertex("event");
                        newEvent.setProperty("event_id", event);
                        newEvent.save();
                        OEdge edge = result.addEdge(newEvent, "attended");
                        edge.save();
                    }
                    rs1.close();

                }
                result.save();
                for (String contact : patient.contact_list) {
                    rs = db.query(query, contact);
                    if (rs.hasNext()) {
                        item = rs.next();
                        if (item.isVertex()) {
                            OVertex  v = item.getVertex().orElse(null);
                            OEdge edge = result.addEdge(v, "contact_with");
                            edge.save();
                        }
                    } else {
                        OVertex newContact = db.newVertex("patient");
                        newContact.setProperty("patient_mrn", contact);
                        newContact.save();
                        OEdge edge = result.addEdge(newContact, "contact_with");
                        edge.save();
                    }
//                    rs.close();
                }
            }
        }
        else
        {
            System.out.println("Creating new patient");
            OVertex result = db.newVertex("patient");
            result.setProperty("testing_id", patient.testing_id);
            result.setProperty("patient_mrn", patient.patient_mrn);
            result.setProperty("patient_name", patient.patient_name);
            result.setProperty("patient_status", patient.patient_status);
            result.setProperty("patient_zipcode", patient.patient_zipcode);
            result.setProperty("patient_status", patient.patient_status);
            result.setProperty("contact_list", patient.contact_list);
            result.save();
            for (String contact : patient.contact_list) {
                rs = db.query(query, contact);
                if (rs.hasNext()) {
                    OResult item = rs.next();
                    if (item.isVertex()) {
                        OVertex v = item.getVertex().get();
                        OEdge edge = result.addEdge(v, "contact_with");
                        edge.save();
                    }
                } else {
                    OVertex newContact = db.newVertex("patient");
                    newContact.setProperty("patient_mrn", contact);
                    newContact.save();
                    OEdge edge = result.addEdge(newContact, "contact_with");
                    edge.save();
                }

            }

        }
        rs.close();
    }

    public void addPatient(int testing_id, String patient_name, String patient_mrn, String patient_zipcode, String patient_status, List<String> contact_list, List<String> event_list) {
        TestingData patient = new TestingData(testing_id, patient_mrn, patient_name, patient_status, patient_zipcode, contact_list, event_list);
        addPatient(patient);
    }

    public void addPatient(String jsonString)
    {
        Gson gson = new Gson();
        System.out.println("jsonString: " + jsonString);
        TestingData patient = gson.fromJson(jsonString, TestingData.class);
//        System.out.println("testing_id: " + patient.testing_id);
//        System.out.println("patient_name: " + patient.patient_name);
//        System.out.println("patient_mrn: " + patient.patient_mrn);
//        System.out.println("patient_zipcode: " + patient.patient_zipcode);
//        System.out.println("patient_status: " + patient.patient_status);
        addPatient(patient);
    }

    public void printString(String s){
        System.out.println(s);
    }

    public void addHospital(HospitalData hospital) {
        String query = "SELECT FROM hospital WHERE hospital_id = ?";
        OResultSet rs = db.query(query, hospital.hospital_id);
        if (rs.hasNext()) {
            OResult item = rs.next();
            if (item.isVertex()) {
                OVertex thisHospital = item.getVertex().get();
                thisHospital.setProperty("hospital_id", hospital.hospital_id);
                thisHospital.save();
                String patientQuery = "SELECT FROM patient WHERE patient_mrn = ?";
                rs = db.query(patientQuery, hospital.patient_mrn);
                if (rs.hasNext()) {
                    item = rs.next();
                    if (item.isVertex()) {
                        OVertex thisPatient = item.getVertex().get();
                        OEdge edge = thisPatient.addEdge(thisHospital, "hospitalized_at");
                        edge.save();
                    }
                } else {
                    OVertex newPatient = db.newVertex("patient");
                    newPatient.setProperty("patient_mrn", hospital.patient_mrn);
                    newPatient.setProperty("patient_name", hospital.patient_name);
                    newPatient.save();
                    OEdge edge = newPatient.addEdge(thisHospital, "hospitalized_at");
                    edge.save();
                }
            } else {
                // create hospital
                // if the patient exists, create edge
                // if the patient does not exist, create patient and edge
                OVertex newHospital = db.newVertex("hospital");
                newHospital.setProperty("hospital_id", hospital.hospital_id);
                newHospital.save();
                String patientQuery = "SELECT FROM patient WHERE patient_mrn = ?";
                rs = db.query(patientQuery, hospital.patient_mrn);
                if (rs.hasNext()) {
                    item = rs.next();
                    if (item.isVertex()) {
                        OVertex thisPatient = item.getVertex().get();
                        OEdge edge = thisPatient.addEdge(newHospital, "hospitalized_at");
                        edge.save();
                    }
                } else {
                    OVertex newPatient = db.newVertex("patient");
                    newPatient.setProperty("patient_mrn", hospital.patient_mrn);
                    newPatient.setProperty("patient_name", hospital.patient_name);
                    newPatient.save();
                    OEdge edge = newPatient.addEdge(newHospital, "hospitalized_at");
                    edge.save();
                }
            }
        }
        rs.close();
    }

    public void addVaccine(VaxData vaccine)
    {
        String query = "SELECT FROM vaccine WHERE  vaccination_id = ?";
        OResultSet rs = db.query(query, vaccine.vaccination_id);
        if (rs.hasNext())
        {
            OResult item = rs.next();
            if (item.isVertex())
            {
                // update vaccine
                OVertex thisVaccine = item.getVertex().get();
                String patientQuery = "SELECT FROM patient WHERE patient_mrn = ?";
                rs = db.query(patientQuery, vaccine.patient_mrn);
                if (rs.hasNext())
                {
                    item = rs.next();
                    if (item.isVertex())
                    {
                        OVertex thisPatient = item.getVertex().get();
                        OEdge edge = thisPatient.addEdge(thisVaccine, "vaccinated_with");
                        edge.save();
                    }
                }
                else {
                    OVertex newPatient = db.newVertex("patient");
                    newPatient.setProperty("patient_mrn", vaccine.patient_mrn);
                    newPatient.save();
                    OEdge edge = newPatient.addEdge(thisVaccine, "vaccinated_with");
                    edge.save();
                }
            }
        }
        else
        {
            // the vaccine does not exist
            // create the vaccine
            // if the patient exists, create edge
            // if the patient does not exist, create patient and edge
            OVertex newVaccine = db.newVertex("vaccine");
            newVaccine.setProperty("vaccination_id", vaccine.vaccination_id);
            newVaccine.save();
            String patientQuery = "SELECT FROM patient WHERE patient_mrn = ?";
            rs = db.query(patientQuery, vaccine.patient_mrn);
            if (rs.hasNext())
            {
                OResult item = rs.next();
                if (item.isVertex())
                {
                    OVertex thisPatient = item.getVertex().get();
                    OEdge edge = thisPatient.addEdge(newVaccine, "vaccinated_with");
                    edge.save();
                }
            }
            else
            {
                OVertex newPatient = db.newVertex("patient");
                newPatient.setProperty("patient_mrn", vaccine.patient_mrn);
                newPatient.setProperty("patient_name", vaccine.patient_name);
                newPatient.save();
                OEdge edge = newPatient.addEdge(newVaccine, "vaccinated_with");
                edge.save();
            }
        }
        rs.close();
    }

    private void addEvent(ODatabaseSession db, String event_id) {
        OVertex result = db.newVertex("event");
        result.setProperty("event_id", event_id);
        result.save();
    }

    public void getContacts( String patient_mrn) {

        String query = "SELECT FROM (TRAVERSE inE(), outE(), inV(), outV() \n" +
                "FROM (SELECT FROM patient WHERE patient_mrn = ?) \n" +
                "WHILE $depth <= 2)\n" +
                "WHERE @class = 'patient'\n";
//        query = "SELECT FROM (TRAVERSE in(), out() \n" +
//                "FROM (SELECT FROM patient WHERE patient_mrn = ?) \n" +
//                "WHILE $depth <= 2)\n" +
//                "WHERE @class = 'patient' AND @rid <> (SELECT FROM patient WHERE patient_mrn = ?).@rid\n";
        OResultSet rs = db.query(query, patient_mrn);

        while (rs.hasNext()) {
            OResult item = rs.next();
            if (! item.getProperty("patient_mrn").equals(patient_mrn)) {
                System.out.println("contact: " + item.getProperty("patient_name"));
            }
        }

        rs.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
    }

    public void getPossibleContacts( String patient_mrn) {

        String query = "SELECT FROM (TRAVERSE inE(), outE(), inV(), outV() \n" +
                "FROM (SELECT FROM patient WHERE patient_mrn = ?) \n" +
                "WHILE $depth <= 2)\n" +
                "WHERE @class = 'patient'\n";


    }

}




