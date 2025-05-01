package auto.inv.micros;

import io.restassured.response.Response;
import auto.restfulapis.EPushNotifsModel;
import auto.utils.connector.RestAssuredConnector;
import auto.utils.general.ConfigUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class POSTEPNotifsHappyTest {
    protected static final Logger logger = LoggerFactory.getLogger(POSTEPNotifsHappyTest.class);
    private String requestUri;
    public String requestBody;
    EPushNotifsModel responseMessage;
    RestAssuredConnector connector;
    UUID generateUUID = UUID.randomUUID();
    String GUID = '{' + generateUUID.toString() + '}';
    public String ORIG_EVENT_DESC = "";
    public String MSG_TYPE = "";
    public String POLICY_NO = "1111111111";
    public String ORIG_APP_ID = "";

    @Before
    public void setupData() {
        EPushNotifsModel requestMessageBody = new EPushNotifsModel();
        requestUri = ConfigUtils.getProperty(ConfigUtils.ConfigKeys.BASE_URL_QA.toString()) +
                ConfigUtils.getProperty(ConfigUtils.ConfigKeys.MIDDLE_URL_E_QA.toString()) +
                ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.END_URL_E_QA));

        requestBody = requestMessageBody.createModel(GUID, ORIG_EVENT_DESC, ORIG_APP_ID, MSG_TYPE, POLICY_NO);

        connector = new RestAssuredConnector();
    }

    @Test
    public void postEPNotifsValidationsHappyTest() {
        RestAssuredConnector connector = new RestAssuredConnector();
        Map<String, String> headers = RestAssuredConnector.setHeaders(ConfigUtils.getProperty(ConfigUtils.ConfigKeys.TOKEN_E.toString()));
        Response ecorrResponse = connector.postRequest(requestUri, headers, requestBody);

        logger.info("requestUri:" + " " + requestUri);
        logger.info("requestBody:" + " " + requestBody);
        logger.info("Response:" + " " + ecorrResponse.getBody().asString());
        assertEquals(ecorrResponse.getStatusCode(), 200);
    }
}
