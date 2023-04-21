package cs505finaltemplate.httpcontrollers;

import com.google.gson.Gson;
import cs505finaltemplate.Launcher;

import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.LinkedHashMap;

@Path("/api")
public class API {

    @Inject
    private javax.inject.Provider<org.glassfish.grizzly.http.server.Request> request;

    private Gson gson;

    public API() {
        gson = new Gson();
    }

    @GET
    @Path("/getteam")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getTeam() {
        String responseString = "{}";
        try {
            // {"team_name":"The DB Dogs","Team_members_sids":"[12028230,12648912]","app_status_code":1}
            Map<String, Object> responseMap = new LinkedHashMap<>();
            responseMap.put("team_name", "The DB Dogs");
            // create ID list
            List<Integer> idList = new ArrayList<>();
            idList.add(12028230);
            idList.add(12648912);
            responseMap.put("Team_members_sids", idList);
            if (Launcher.graphDBEngine != null && Launcher.cepEngine != null) {
                responseMap.put("app_status_code", 1);
            } else {
                responseMap.put("app_status_code", 0);
            }

            responseString = gson.toJson(responseMap);

        } catch (Exception ex) {
            return printException(ex);
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/getlastcep")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAccessCount() {
        String responseString = "{}";
        try {
            // generate a response
            responseString = gson.toJson(Launcher.lastCEPOutput);
        } catch (Exception ex) {
            return printException(ex);
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    // Reset all DBs
    @GET
    @Path("/reset")
    @Produces(MediaType.APPLICATION_JSON)
    public Response reset() {
        String responseString = "{}";
        try {

            Launcher.graphDBEngine.clearData();
            Launcher.cepEngine.cleanDB();

            Map<String, Integer> responseMap = new HashMap<>();
            responseMap.put("reset_status_code", 1);
            responseString = gson.toJson(responseMap);
        } catch (Exception ex) {
            return printException(ex);
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    // CEP API functions
    @GET
    @Path("/zipalertlist")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getZipAlertList() {
        // We define alert state as a growth of 2X over two batches of messages.
        String responseString = "{}";
        try {
            Map<String, Object> responseMap = new HashMap<>();
            responseMap.put("ziplist", Launcher.alert_list);
            responseString = gson.toJson(responseMap);
            // System.out.println("zip alert list response json: " + responseString + "\n");// @todo: remove debug line
        } catch (Exception ex) {
            return printException(ex);
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/alertlist")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAlertList() {
        // state_status = 0 = not alert, 1 = alert
        // alert on statewide when at least five zipcodes are in alert state
        String responseString = "{}";
        try {
            Map<String, Integer> responseMap = new HashMap<>();
            responseMap.put("state_status", (Launcher.cepEngine.getStateAlert()) ? 1 : 0);
            responseString = gson.toJson(responseMap);
        } catch (Exception ex) {
            return printException(ex);
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    // graph API functions
    @GET
    @Path("/getconfirmedcontacts/{patient_mrn}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getConfirmedContacts(@PathParam("patient_mrn") String patient_mrn) {
        String responseString = "{}";
        try {
            responseString = Launcher.graphDBEngine.getConfirmedContacts(patient_mrn);

        } catch (Exception ex) {
            return printException(ex);
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/getpossiblecontacts/{patient_mrn}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPossibleContacts(@PathParam("patient_mrn") String patient_mrn) {
        String responseString = "{}";
        try {
            responseString = Launcher.graphDBEngine.getPossibleContacts(patient_mrn);
        } catch (Exception ex) {
            return printException(ex);
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/getpatientstatus/{hospital_id}")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPatientStatus(@PathParam("hospital_id") String hospital_id) {
        String responseString = "{}";
        try {
            responseString = Launcher.graphDBEngine.getPatientStatusofOneHospital(hospital_id);
        } catch (Exception ex) {
            return printException(ex);
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/getpatientstatus")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPatientStatus() {
        String responseString = "{}";
        try {
            responseString = Launcher.graphDBEngine.getPatientStatusofAllHospital();
        } catch (Exception ex) {
            return printException(ex);
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    private Response printException(Exception ex) {

        StringWriter sw = new StringWriter();
        ex.printStackTrace(new PrintWriter(sw));
        String exceptionAsString = sw.toString();
        ex.printStackTrace();

        return Response.status(500).entity(exceptionAsString).build();
    }
}
