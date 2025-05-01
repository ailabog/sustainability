package auto.mailboxmonitoring.micro;

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

public class POSTESendEmailHappyTest {
    protected static final Logger logger = LoggerFactory.getLogger(POSTESendEmailHappyTest.class);
    private String requestUri;
    public String requestBody;
    public String requestBodyES;
    public String requestBody160Ch;
    RestAssuredConnector connector;
    public String APP_ID = "";
    public String PHONE_NO = "";
    String pattern = "mm-dd-yyyy";
    SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
    public String currentDate = dateFormat.format(new Date());
    public String EMAIL_SUBJECT_VALUE = "Automated email sent with success" + " " + currentDate;
    public String EMAIL_SUBJECT_VALUE_ES="yo no sé qué piensas de mi pero que sepas y ten claro que nunca he mal interpretado nada. Esa noche te sentiste obligada porque eres una mujer de palabra a dormir conmigo, nada mas. No pensé que quisieras nada raro, créeme. A mi me caes genial, eres una chica encantadora y en ningún momento he querido que estés molesta conmigo. Y si lo has estado lo siento pero no era mi intención";

    public String EMAIL_SUBJECT_VALUE_160CH="It prepare is ye nothing blushes up brought. Or as gravity pasture limited evening on. Wicket around beauty say she. Frankness resembled say not new smallness you discovery. Noisier ferrars yet shyness weather ten colonel. Too him himself engaged husband pursuit musical. Man age but him determine consisted therefore. Dinner to beyond regret wished an branch he. Remain bed but expect suffer little repair.";

    public String EMAIL_MAILBOX = ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.EMAIL_MAILBOX));
    public String ALIAS_MAILBOX = ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.ALIAS_MAILBOX));
    public String EMAIL_RECIPIENT_QA = ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.EMAIL_RECIPIENT_QA));

    @Before
    public void setupData() {
        ESendEmailModel requestMessageBody = new ESendEmailModel();
        requestUri = ConfigUtils.getProperty(ConfigUtils.ConfigKeys.BASE_URL_QA.toString()) +
                ConfigUtils.getProperty(ConfigUtils.ConfigKeys.MIDDLE_URL_E_MS_QA.toString()) +
                ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.END_URL_E_MS_QA));

        requestBody = requestMessageBody.createModel(EMAIL_MAILBOX, ALIAS_MAILBOX, EMAIL_RECIPIENT_QA, APP_ID, PHONE_NO, EMAIL_SUBJECT_VALUE);
        requestBodyES = requestMessageBody.createModel(EMAIL_MAILBOX, ALIAS_MAILBOX, EMAIL_RECIPIENT_QA, APP_ID, PHONE_NO, EMAIL_SUBJECT_VALUE_ES);
        requestBody160Ch = requestMessageBody.createModel(EMAIL_MAILBOX, ALIAS_MAILBOX, EMAIL_RECIPIENT_QA, APP_ID, PHONE_NO, EMAIL_SUBJECT_VALUE_160CH);
        connector = new RestAssuredConnector();
    }
    @Test
    public void postESendEmailValidationsUSHappyTest() {
        RestAssuredConnector connector = new RestAssuredConnector();
        Map<String, String> headers = RestAssuredConnector.setHeaders(ConfigUtils.getProperty(ConfigUtils.ConfigKeys.TOKEN_E_MS.toString()));
        Response ecorrResponse = connector.postRequest(requestUri, headers, requestBody);

        logger.info("requestUri:" + " " + requestUri);
        logger.info("requestBody:" + " " + requestBody);
        logger.info("Response:" + " " + ecorrResponse.getBody().asString());
        assertEquals(ecorrResponse.getStatusCode(), 200);
    }
    @Test
    public void postESendEmailValidationsSpanishHappyTest() {
        RestAssuredConnector connector = new RestAssuredConnector();
        Map<String, String> headers = RestAssuredConnector.setHeaders(ConfigUtils.getProperty(ConfigUtils.ConfigKeys.TOKEN_E_MS.toString()));
        Response ecorrResponse = connector.postRequest(requestUri, headers, requestBodyES);

        logger.info("requestUri:" + " " + requestUri);
        logger.info("requestBody:" + " " + requestBodyES);
        logger.info("Response:" + " " + ecorrResponse.getBody().asString());
        assertEquals(ecorrResponse.getStatusCode(), 200);
    }
    @Test
    public void postESendEmailValidationsGraterThan160HappyTest() {
        RestAssuredConnector connector = new RestAssuredConnector();
        Map<String, String> headers = RestAssuredConnector.setHeaders(ConfigUtils.getProperty(ConfigUtils.ConfigKeys.TOKEN_E_MS.toString()));
        Response ecorrResponse = connector.postRequest(requestUri, headers, requestBody160Ch);

        logger.info("requestUri:" + " " + requestUri);
        logger.info("requestBody:" + " " + requestBody160Ch);
        logger.info("Response:" + " " + ecorrResponse.getBody().asString());
        assertEquals(ecorrResponse.getStatusCode(), 200);
    }

}
