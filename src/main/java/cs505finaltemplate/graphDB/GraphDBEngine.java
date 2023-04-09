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
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.db.ODatabaseType;



public class GraphDBEngine {
    private OrientDB orient;
    private ODatabaseSession db;
    private String dbName;

    //!!! CODE HERE IS FOR EXAMPLE ONLY, YOU MUST CHECK AND MODIFY!!!
    public GraphDBEngine(String dbName) {

        this.dbName = dbName;

        this.orient = new OrientDB("remote:localhost","admin","admin", OrientDBConfig.defaultConfig());
        orient.create(dbName,ODatabaseType.PLOCAL);
        this.db = orient.open(dbName, "admin", "admin");

        db.close();
        orient.close();

//        OrientDB orient = new OrientDB("remote:localhost", OrientDBConfig.defaultConfig());
//        ODatabaseSession db = orient.open("test", "root", "rootpwd");

        System.out.println("GraphDBEngine constructor");

        //launch a docker container for orientdb, don't expect your data to be saved unless you configure a volume
        //docker run -d --name orientdb -p 2424:2424 -p 2480:2480 -e ORIENTDB_ROOT_PASSWORD=rootpwd orientdb:3.0.0

        //use the orientdb dashboard to create a new database
        //see class notes for how to use the dashboard

//        clearDB(db);
//
//        //create classes
//        OClass patient = db.getClass("patient");
//
//
//        if (patient == null) {
//            patient = db.createVertexClass("patient");
//        }
//
//        if (patient.getProperty("patient_mrn") == null) {
//            patient.createProperty("patient_mrn", OType.STRING);
//            patient.createIndex("patient_name_index", OClass.INDEX_TYPE.NOTUNIQUE, "patient_mrn");
//        }
//
//        if (db.getClass("contact_with") == null) {
//            db.createEdgeClass("contact_with");
//        }
//
//        OClass hospital = db.getClass("hospital");
//        if (hospital == null) {
//            hospital = db.createVertexClass("hospital");
//        }
//
//        if(hospital.getProperty("hospital_id") == null) {
//            hospital.createProperty("hospital_id", OType.STRING);
//            hospital.createIndex("hospital_id_index", OClass.INDEX_TYPE.NOTUNIQUE, "hospital_id");
//        }
//
//        if (db.getClass("hospitalized_at") == null) {
//            db.createEdgeClass("hospitalized_at");
//        }


//        OVertex patient_0 = createPatient(db, "mrn_0");
//        OVertex patient_1 = createPatient(db, "mrn_1");
//        OVertex patient_2 = createPatient(db, "mrn_2");
//        OVertex patient_3 = createPatient(db, "mrn_3");

        //patient 0 in contact with patient 1
//        OEdge edge1 = patient_0.addEdge(patient_1, "contact_with");
//        edge1.save();
        //patient 2 in contact with patient 0
//        OEdge edge2 = patient_2.addEdge(patient_0, "contact_with");
//        edge2.save();

        //you should not see patient_3 when trying to find contacts of patient 0
//        OEdge edge3 = patient_3.addEdge(patient_2, "contact_with");
//        edge3.save();

//        getContacts(db, "mrn_0");

//        db.close();
//        orient.close();

    }
}
//
//    public void addPatient(TestingData patient) {
//        // If the node already exists, update it
//        // If the node does not exist, create it
//        // If the node has a contact, create an edge between the two nodes
//        String query = "SELECT FROM patient WHERE patient_mrn = ?";
//        OResultSet rs = db.query(query, patient.patient_mrn);
//        if (rs.hasNext()) {
//            OResult item = rs.next();
//            if (item.isVertex()) {
//                OVertex result = item.getVertex().get();
//                result.setProperty("testing_id", patient.testing_id);
//                result.setProperty("patient_mrn", patient.patient_mrn);
//                result.setProperty("patient_name", patient.patient_name);
//                result.setProperty("patient_status", patient.patient_status);
//                result.setProperty("patient_zipcode", patient.patient_zipcode);
//                result.setProperty("patient_status", patient.patient_status);
//                result.setProperty("contact_list", patient.contact_list);
//                result.save();
//                for (String contact : patient.contact_list) {
//                    rs = db.query(query, contact);
//                    if (rs.hasNext()) {
//                        item = rs.next();
//                        if (item.isVertex()) {
//                            OVertex  v = item.getVertex().orElse(null);
//                            OEdge edge = result.addEdge(v, "contact_with");
//                            edge.save();
//                        }
//                    } else {
//                        OVertex newContact = db.newVertex("patient");
//                        newContact.setProperty("patient_mrn", contact);
//                        newContact.save();
//                        OEdge edge = result.addEdge(newContact, "contact_with");
//                        edge.save();
//                    }
////                    rs.close();
//                }
//            }
//            rs.close();
//        } else {
//            OVertex result = db.newVertex("patient");
//            result.setProperty("testing_id", patient.testing_id);
//            result.setProperty("patient_mrn", patient.patient_mrn);
//            result.setProperty("patient_name", patient.patient_name);
//            result.setProperty("patient_status", patient.patient_status);
//            result.setProperty("patient_zipcode", patient.patient_zipcode);
//            result.setProperty("patient_status", patient.patient_status);
//            result.setProperty("contact_list", patient.contact_list);
//            result.save();
//            for (String contact : patient.contact_list) {
//                rs = db.query(query, contact);
//                if (rs.hasNext()) {
//                    OResult item = rs.next();
//                    if (item.isVertex()) {
//                        OVertex v = item.getVertex().get();
//                        OEdge edge = result.addEdge(v, "contact_with");
//                        edge.save();
//                    }
//                } else {
//                    OVertex newContact = db.newVertex("patient");
//                    newContact.setProperty("patient_mrn", contact);
//                    newContact.save();
//                    OEdge edge = result.addEdge(newContact, "contact_with");
//                    edge.save();
//                }
//                rs.close();
//            }
//        }
//    }
//
//    public void addHospital(HospitalData hospital){
//        String query = "SELECT FROM hospital WHERE  = ?";
//        OResultSet rs = db.query(query,hospital.patient_mrn);
//        OResult item = rs.next();
//        if(item.isVertex())
//        {
//            OVertex thisHospital = item.getVertex().get();
//            thisHospital.setProperty("hospital_id", hospital.hospital_id);
//            thisHospital.save();
//            String patientQuery = "SELECT FROM patient WHERE patient_mrn = ?";
//            rs = db.query(patientQuery, hospital.patient_mrn);
//            if(rs.hasNext())
//            {
//                item = rs.next();
//                if(item.isVertex())
//                {
//                    OVertex thisPatient = item.getVertex().get();
//                    OEdge edge = thisPatient.addEdge(thisHospital, "hospitalized_at");
//                    edge.save();
//                }
//            }
//            else
//            {
//                OVertex newPatient = db.newVertex("patient");
//                newPatient.setProperty("patient_mrn", hospital.patient_mrn);
//                newPatient.setProperty("patient_name", hospital.patient_name);
//                newPatient.save();
//                OEdge edge = newPatient.addEdge(thisHospital, "hospitalized_at");
//                edge.save();
//            }
//
//        }
//
//    }
//
//    public String checkContacts(String patient_mrn) {
//
//        return null;
//    }
//
//
//    private OVertex createPatient(ODatabaseSession db, String patient_mrn) {
//        OVertex result = db.newVertex("patient");
//        result.setProperty("patient_mrn", patient_mrn);
//        result.save();
//        return result;
//    }
//
//    private OVertex createHospital(ODatabaseSession db, String hospital_id) {
//        OVertex result = db.newVertex("hospital");
//        result.setProperty("hospital_id", hospital_id);
//        result.save();
//        return result;
//    }
//
//    public String getContactList(String patient_mrn){
//        String query = "TRAVERSE inE(), outE(), inV(), outV() " +
//                "FROM (select from patient where patient_mrn = ?) " +
//                "WHILE $depth <= 2";
//        OResultSet rs = db.query(query, patient_mrn);
//        String contactList = "";
//        while (rs.hasNext()) {
//            OResult item = rs.next();
//            contactList += item.getProperty("patient_mrn") + ",";
//        }
//        rs.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
//        return contactList ;
//    }
//
//    private void getContacts(ODatabaseSession db, String patient_mrn) {
//
//        String query = "TRAVERSE inE(), outE(), inV(), outV() " +
//                "FROM (select from patient where patient_mrn = ?) " +
//                "WHILE $depth <= 2";
//        OResultSet rs = db.query(query, patient_mrn);
//
//        while (rs.hasNext()) {
//            OResult item = rs.next();
//            System.out.println("contact: " + item.getProperty("patient_mrn"));
//        }
//
//        rs.close(); //REMEMBER TO ALWAYS CLOSE THE RESULT SET!!!
//    }
//
//
//
//    private void clearDB(ODatabaseSession db) {
//        String query = "DELETE VERTEX FROM patient";
//        System.out.println("Deleting all vertices...");
//        db.command(query);
//    }
//
//    public void resetDB()
//    {
//        clearDB(db);
//    }
//
//}
