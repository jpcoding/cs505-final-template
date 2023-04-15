package cs505finaltemplate.Topics;
import java.util.List;


public class VaxData{
    // "vaccination_id": 6,
    // "patient_name": "Henrietta Cook",
    // "patient_mrn": "1def2240-b4e8-11ec-a016-ac87a3187c5f"
    public String vaccination_id;
    public String patient_name;
    public String patient_mrn;

    public VaxData(String vaccination_id, String patient_name, String patient_mrn) {
        this.vaccination_id = vaccination_id;
        this.patient_name = patient_name;
        this.patient_mrn = patient_mrn;
    }

    public boolean isValid() {
        return !(vaccination_id == null || patient_name == null || patient_mrn == null);
    }
}