package auto.inv.micros;

import io.restassured.response.Response;
import auto.utils.connector.RestAssuredConnector;
import auto.utils.general.ConfigUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import static org.junit.Assert.assertEquals;

public class GETCstIdHappyTest {
    protected static final Logger logger = LoggerFactory.getLogger(GETCstIdHappyTest.class);
    private String requestUri;
    private String CST_ID = "202c95cf-0147-c36a-7fff-1286aa06455e";

    @Test
    public void GETCstIdHappyTest() {
        requestUri = ConfigUtils.getProperty(ConfigUtils.ConfigKeys.BASE_URL_KONG_QA.toString()) +
                ConfigUtils.getProperty(ConfigUtils.ConfigKeys.MIDDLE_URL_CST_ID_QA.toString()) + CST_ID;

        RestAssuredConnector connector = new RestAssuredConnector();
        Map<String, String> headers = RestAssuredConnector.setHeaders(ConfigUtils.getProperty(ConfigUtils.ConfigKeys.TOKEN_CST_ID.toString()));
        Response customerIdResponse = connector.getRequest(requestUri, headers);
        logger.info("requestUri:" + " " + requestUri);

        String responseBody_customerId = customerIdResponse.getBody().asString();
        logger.info("Response:" + " " + responseBody_customerId);

        assertEquals(200, customerIdResponse.getStatusCode());
        logger.info("Accessing memberGUID..");

        JSONObject jsonResponse = new JSONObject(responseBody_customerId);

        JSONArray customers = jsonResponse.getJSONArray("customers");
        String  memberGUID = customers.getJSONObject(0).getString("memberGUID");

        logger.info("memberGUID: " + memberGUID);
    }
}
