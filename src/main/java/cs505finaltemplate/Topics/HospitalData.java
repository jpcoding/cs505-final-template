package cs505finaltemplate.Topics;

import java.util.List;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import com.google.gson.Gson; 
import com.google.gson.GsonBuilder;

public class HospitalData {
    // "hospital_id": "12142728",
    // "patient_name": "Henrietta Cook",
    // "patient_mrn": "1def2240-b4e8-11ec-a016-ac87a3187c5f",
    // "patient_status": 2
    public String hospital_id;
    public String patient_name;
    public String patient_mrn;
    public int patient_status;

    public HospitalData(String hospital_id, String patient_name, String patient_mrn, int patient_status) {
        this.hospital_id = hospital_id;
        this.patient_name = patient_name;
        this.patient_mrn = patient_mrn;
        this.patient_status = patient_status;
    }

    public boolean isValid() {
        boolean patient_status_valid = (patient_status == 1) || (patient_status == 2) || (patient_status == 3);
        return hospital_id != null && patient_name != null && patient_mrn != null && patient_status_valid;
    }
}