package auto.inv.micros_db;

import io.restassured.response.Response;
import auto.db.DBConnections;
import auto.restfulapis.EPushNotifsModel;
import auto.utils.connector.RestAssuredConnector;
import auto.utils.general.ConfigUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.assertEquals;

public class POSTEPNotifsDBValidationsTest {
    protected static final Logger logger = LoggerFactory.getLogger(POSTEPNotifsDBValidationsTest.class);

    public String requestUri;
    public String requestBody;
    RestAssuredConnector connector;
    DBConnections dbConn;
    UUID generateUUID = UUID.randomUUID();
    String GUID = '{' + generateUUID.toString() + '}';
    public String ORIG_EVENT_DESC = "";
    public String MSG_TYPE = "";
    public String POLICY_NO = "1111111111";
    public String ORIG_APP_ID = "";
    public String QUERY = "SELECT * FROM message WHERE type = \"push\" AND source = \"SOURCE\" AND JSON_EXTRACT(payload, \"$.triggers.onsuccess\") = \"push-android\" AND  JSON_EXTRACT(payload, \"$.content.sharedData.origEvId\")= ?";

    @Before
    public void setupData() {
        EPushNotifsModel requestMessageBody = new EPushNotifsModel();
        dbConn = new DBConnections();
        requestUri = ConfigUtils.getProperty(ConfigUtils.ConfigKeys.BASE_URL_QA.toString()) +
                ConfigUtils.getProperty(ConfigUtils.ConfigKeys.MIDDLE_URL_E_QA.toString()) +
                ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.END_URL_E_QA));

        requestBody = requestMessageBody.createModel(GUID, ORIG_EVENT_DESC, ORIG_APP_ID, MSG_TYPE, POLICY_NO);

        connector = new RestAssuredConnector();
    }

    @Test
    public void postEPNotifsDBValidationsHappyTest() throws SQLException {
        RestAssuredConnector connector = new RestAssuredConnector();
        Map<String, String> headers = RestAssuredConnector.setHeaders(ConfigUtils.getProperty(ConfigUtils.ConfigKeys.TOKEN_E.toString()));
        Response mmpayResponse = connector.postRequest(requestUri, headers, requestBody);

        logger.info("requestUri:" + " " + requestUri);
        logger.info("requestBody:" + " " + requestBody);
        logger.info("Response:" + " " + mmpayResponse.getBody().asString());
        assertEquals(mmpayResponse.getStatusCode(), 200);

        ResultSet rs = dbConn.dbConn(ConfigUtils.getProperty(ConfigUtils.ConfigKeys.E_DB_URL_QA.name()), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.E_DB_USERNAME_QA)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.E_DB_PASSWORD_QA)), QUERY, GUID);

        if (rs.next()) {
            String typeDB = rs.getString("type");
            String sourceDB = rs.getString("source");
            String payloadDB = rs.getString("payload");
            String received_dateDB = rs.getString("received_date");

            JSONObject jsonPayloadDB = new JSONObject(payloadDB);
            JSONObject sharedDataDB = jsonPayloadDB.getJSONObject("content").getJSONObject("sharedData");

            String originalEventCorrelationIDDB = sharedDataDB.getString("originalEventCorrelationID");
            String applicationIdDB = jsonPayloadDB.getString("applicationId");
            String overwriteMessageTypeDB = sharedDataDB.getString("overwriteMessageType");
            String originalEventInitiatorDescriptionDB = sharedDataDB.getString("originalEventInitiatorDescription");
            String originalApplicationIdDB = sharedDataDB.getString("originalApplicationId");
            String policyNumberDB = sharedDataDB.getString("policyNumber");
            String originalEventRequestIDDB = sharedDataDB.getString("originalEventRequestID");
            String originalEventSourceDescriptionDB = sharedDataDB.getString("originalEventSourceDescription");
            String originalEventSourceDB = sharedDataDB.getString("originalEventSource");
            String originalEventInitiatorDB = sharedDataDB.getString("originalEventInitiator");
            String originalEventDateTimeDB = sharedDataDB.getString("originalEventDateTime");
            String userIdDB = sharedDataDB.getString("userId");
            String fileNameDB = jsonPayloadDB.getJSONArray("items").getJSONObject(0).getJSONObject("source").getJSONObject("parameters").getString("fileName");
            String soundDB = jsonPayloadDB.getJSONArray("items").getJSONObject(0).getJSONObject("source").getJSONObject("data").getString("sound");
            String templateTypeDB = jsonPayloadDB.getJSONArray("items").getJSONObject(0).getString("templateType");
            String sourceTypeDB = jsonPayloadDB.getJSONArray("items").getJSONObject(0).getJSONObject("source").getString("sourceType");
            JSONArray onsuccessDB = jsonPayloadDB.getJSONObject("triggers").getJSONArray("onsuccess");
            String messageTypeDB = jsonPayloadDB.getString("messageType");

            if (originalEventCorrelationIDDB.equals(GUID)) {

                assertThat(applicationIdDB).isEqualTo(ORIG_APP_ID);
                assertThat(overwriteMessageTypeDB).isEqualTo(MSG_TYPE);
                assertThat(originalEventInitiatorDescriptionDB).isEqualTo(ORIG_EVENT_DESC);
                assertThat(originalEventSourceDescriptionDB).isEqualTo(ORIG_EVENT_DESC);
                assertThat(originalEventSourceDB).isEqualTo("comm-orch");
                assertThat(originalEventRequestIDDB).isEqualTo(GUID);
                assertThat(policyNumberDB).isEqualTo(POLICY_NO);
                assertThat(originalEventInitiatorDB).isEqualTo(ORIG_APP_ID);
                assertThat(fileNameDB).isEqualTo("");
                assertThat(templateTypeDB).isEqualTo("");
                assertThat(sourceTypeDB).isEqualTo("");
                assertThat(originalApplicationIdDB).isEqualTo(ORIG_APP_ID);
                assertThat(messageTypeDB).isEqualTo(MSG_TYPE);
            }
            else {
                logger.info("Something must be really wrong with this data");
            }
        }
        dbConn.closeConnection(rs.getStatement().getConnection());
        }
    }

