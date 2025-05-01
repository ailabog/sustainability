package massmutual.ebill.micros;

import io.restassured.response.Response;
import massmutual.utils.connector.RestAssuredConnector;
import massmutual.utils.general.ConfigUtils;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Map;
import static org.junit.Assert.assertEquals;

public class GETAgrCstHappyTest{
    protected static final Logger logger = LoggerFactory.getLogger(GETAgrCstHappyTest.class);
    private String requestUri;
    private String END_URL_AGREMENT_QA="111111%7C%7C%7CFDPX00";

    @Test
    public void getAgreementCustomerHappyTest() {
        requestUri = ConfigUtils.getProperty(ConfigUtils.ConfigKeys.BASE_URL_KONG_QA.toString()) +
                ConfigUtils.getProperty(ConfigUtils.ConfigKeys.MIDDLE_URL_AGREEMENT_QA.toString()) + END_URL_AGREMENT_QA;
        logger.info("requestUri:" + " " + requestUri);

        RestAssuredConnector connector = new RestAssuredConnector();
        Map<String, String> headers = RestAssuredConnector.setHeaders(ConfigUtils.getProperty(ConfigUtils.ConfigKeys.TOKEN_AGREEMENT_CUSTOMER.toString()));
        Response agreementResponse = connector.getRequest(requestUri, headers);

        String responseBody_agreementKey = agreementResponse.getBody().asString();
        logger.info("Response:" + " " + responseBody_agreementKey);

        assertEquals(200, agreementResponse.getStatusCode());
        JSONObject jsonResponse = new JSONObject(responseBody_agreementKey);

        JSONArray agreements = jsonResponse.getJSONArray("agreements");
        JSONObject customer = agreements.getJSONObject(0).getJSONArray("agreementCustomers").getJSONObject(0);

        JSONArray agreementsArray = jsonResponse.getJSONArray("agreements");
        String agreementKeyMS = agreementsArray.getJSONObject(0).getString("agreementKey");

        String roleType = customer.getString("roleType");
        String memberGUIDAgreementCustomer = customer.getString("memberGUID");

        logger.info("Fields extracted from the ms:" + "agreement key: " + agreementKeyMS + " " + "roleType: " + roleType + " " + "member GUID:" + memberGUIDAgreementCustomer);
    }
}
