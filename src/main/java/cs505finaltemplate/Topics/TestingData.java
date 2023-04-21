package cs505finaltemplate.Topics;

import java.util.List;

public class TestingData {

    public int testing_id;
    public String patient_name;
    public String patient_mrn;
    public String patient_zipcode;
    public String patient_status;
    public List<String> contact_list;
    public List<String> event_list;

    public TestingData(int testing_id, String patient_name, String patient_mrn, String patient_zipcode,
            String patient_status, List<String> contact_list, List<String> event_list) {
        // default constructor
        this.testing_id = testing_id;
        this.patient_name = patient_name;
        this.patient_mrn = patient_mrn;
        this.patient_zipcode = patient_zipcode;
        this.patient_status = patient_status;
        this.contact_list = contact_list;
        this.event_list = event_list;
    }

    public boolean isValid() {

        boolean patient_status_valid = this.patient_status != null
                && (this.patient_status.equals("1") || this.patient_status.equals("0"));

        return this.patient_name != null && this.patient_mrn != null && this.patient_zipcode != null
                && this.contact_list != null && this.event_list != null && patient_status_valid;
    }
}
