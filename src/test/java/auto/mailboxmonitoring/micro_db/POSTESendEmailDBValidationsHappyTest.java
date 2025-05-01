package auto.mailboxmonitoring.micro_db;

import io.restassured.response.Response;
import auto.restfulapis.ESendEmailModel;
import auto.utils.connector.RestAssuredConnector;
import auto.utils.general.ConfigUtils;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import static org.junit.Assert.assertEquals;

public class POSTESendEmailDBValidationsHappyTest {
    protected static final Logger logger = LoggerFactory.getLogger(POSTESendEmailDBValidationsHappyTest.class);
    private String requestUri;
    public String requestBodyUS;
    RestAssuredConnector connector;
    public String APP_ID = "MAILM";
    public String PHONE_NO = "+12672133096";
    String pattern = "mm-dd-yyyy";
    SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
    public String currentDate = dateFormat.format(new Date());
    public String EMAIL_SUBJECT_VALUE = "Automated email sent with success" + " " + currentDate;
    public String EMAIL_MAILBOX = ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.EMAIL_MAILBOX));
    public String ALIAS_MAILBOX = ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.ALIAS_MAILBOX));
    public String EMAIL_RECIPIENT_QA = ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.EMAIL_RECIPIENT_QA));

    @Before
    public void setupData() {
        ESendEmailModel requestMessageBody = new ESendEmailModel();
        requestUri = ConfigUtils.getProperty(ConfigUtils.ConfigKeys.BASE_URL_QA.toString()) +
                ConfigUtils.getProperty(ConfigUtils.ConfigKeys.MIDDLE_URL_E_MS_QA.toString()) +
                ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.END_URL_E_MS_QA));

        requestBodyUS = requestMessageBody.createModel(EMAIL_MAILBOX, ALIAS_MAILBOX, EMAIL_RECIPIENT_QA, APP_ID, PHONE_NO, EMAIL_SUBJECT_VALUE);

        connector = new RestAssuredConnector();
    }
    @Test
    public void postESendEmailUSDBValidationsHappyTest() {
        RestAssuredConnector connector = new RestAssuredConnector();
        Map<String, String> headers = RestAssuredConnector.setHeaders(ConfigUtils.getProperty(ConfigUtils.ConfigKeys.TOKEN_E_MS.toString()));
        Response EResponse = connector.postRequest(requestUri, headers, requestBodyUS);

        logger.info("requestUri:" + " " + requestUri);
        logger.info("requestBody:" + " " + requestBodyUS);
        logger.info("Response:" + " " + EResponse.getBody().asString());
        assertEquals(EResponse.getStatusCode(), 200);
    }


}


}
