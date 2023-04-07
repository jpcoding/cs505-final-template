package cs505finaltemplate.httpcontrollers;

import com.google.gson.Gson;
import cs505finaltemplate.Launcher;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.HeaderParam;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

// TODO 
// APIS 
// getteam 
// reset 
//

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
            System.out.println("WHAT");
            Map<String, String> responseMap = new HashMap<>();
            responseMap.put("team_name", "templateTeam");
            responseMap.put("Team_members_sids", "[12028230,PuJiaosID]");// @todo: put PuJiao's SID in response string
            responseMap.put("app_status_code", "0");

            responseString = gson.toJson(responseMap);

        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    // @todo: do we need this function...?
    @GET
    @Path("/rest")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getRest() {
        String responseString = "{}";
        try {
            System.out.println("WHAT");
            Map<String, String> responseMap = new HashMap<>();
            responseMap.put("team_name", "DB Dogs");
            responseMap.put("Team_members_sids", "[0]");
            responseMap.put("app_status_code", "0");
            responseString = gson.toJson(responseMap);
        } catch (Exception ex) {
            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();
            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/getlastcep")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAccessCount(@HeaderParam("X-Auth-API-Key") String authKey) {
        String responseString = "{}";
        try {

            // generate a response
            Map<String, String> responseMap = new HashMap<>();
            responseMap.put("lastoutput", Launcher.lastCEPOutput);
            responseString = gson.toJson(responseMap);

        } catch (Exception ex) {

            StringWriter sw = new StringWriter();
            ex.printStackTrace(new PrintWriter(sw));
            String exceptionAsString = sw.toString();
            ex.printStackTrace();

            return Response.status(500).entity(exceptionAsString).build();
        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/reset")
    @Produces(MediaType.APPLICATION_JSON)
    public Response reset(@HeaderParam("X-Auth-API-Key") String authKey) {
        // @todo: status code 0 means fail, 1 means success. make that work.
        String responseString = "{\"reset_status_code\": }";
        try {

            Launcher.graphDBEngine.resetDB();

        } catch (Exception ex) {

        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    // CEP API functions
    @GET
    @Path("/zipalertlist")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getZipAlertList(@HeaderParam("X-Auth-API-Key") String authKey) {
        // @todo: implement
        // We define alert state as a growth of 2X over two batches of messages.
        String responseString = "{\"ziplist\": }";
        try {

        } catch (Exception ex) {

        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/alertlist")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getAlertList(@HeaderParam("X-Auth-API-Key") String authKey) {
        // @todo: implement - state_status = 0 = not alert, 1 = alert
        // alert on statewide when at least five zipcodes are in alert state
        // (based on RT1) within the same 15 second window.
        String responseString = "{\"state_status\": }";
        try {

        } catch (Exception ex) {

        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    // graph API functions
    @GET
    @Path("/getconfirmedcontacts")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getConfirmedContacts(@HeaderParam("X-Auth-API-Key") String authKey) {
        // @todo: implement
        String responseString = "{\"ziplist\": }";
        try {

        } catch (Exception ex) {

        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/getpossiblecontacts")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPossibleContacts(@HeaderParam("X-Auth-API-Key") String authKey) {
        // @todo: implement
        String responseString = "{\"\": }";
        try {

        } catch (Exception ex) {

        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }

    @GET
    @Path("/getpatientstatus")
    @Produces(MediaType.APPLICATION_JSON)
    public Response getPatientStatus(@HeaderParam("X-Auth-API-Key") String authKey) {
        // @todo: implement
        String responseString = "{\"\": }";
        try {

        } catch (Exception ex) {

        }
        return Response.ok(responseString).header("Access-Control-Allow-Origin", "*").build();
    }
}
