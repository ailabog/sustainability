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

public class POSTEendEmailNegativeTest {
    protected static final Logger logger = LoggerFactory.getLogger(POSTEcorrSendEmailNegativeTest.class);
    private String requestUri;
    public String requestBodyRo;
    public String requestBodyES;
    public String requestBodyPO;
    public String requestBodyChinesse;
    public String requestBodyDummy;
    public String requestBodyWildCards;
    RestAssuredConnector connector;
    String pattern = "mm-dd-yyyy";
    SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
    String currentDate = dateFormat.format(new Date());
    public String APP_ID = "MAILM";
    public String PHONE_NO_China = "自动电子邮件今天已成功发送";
    public String EMAIL_SUBJECT_VALUE_China = "自动电子邮件今天已成功发送 +8613910998888 自动电子邮件今天已成功发送" + " " + currentDate;
    public String PHONE_NO_ES = "+34919934755";
    public String EMAIL_SUBJECT_VALUE_ES = "Este es un correo electrónico automatizado." + " " + currentDate;
    public String PHONE_NO_RO = "+40727890123";
    public String EMAIL_SUBJECT_VALUE_RO = "Acesta este un email automat." + " " + currentDate;
    public String PHONE_NO_PO = "+351212111333";
    public String EMAIL_SUBJECT_VALUE_PO = "O Porto é, frequentemente, referido como a cidade portuguesa com o temperamento mais centro-europeu, devido ao bucolismo requintado do seu espaço urbano" + " " + currentDate;
    public String APP_ID_DUMMY = "DUMMY";
    public String EMAIL_SUBJECT_VALUE_DUMMY = "DUMMY";
    public String PHONE_NO_DUMMY = "DUMMY";

    public String APP_ID_WILDCARDS = "@#$%^";
    public String EMAIL_SUBJECT_VALUE_WILDCARDS = "@#$%^";
    public String PHONE_NO_WILDCARDS = "@#$%^";

    public String EMAIL_MAILBOX = ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.EMAIL_MAILBOX));
    public String ALIAS_MAILBOX = ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.ALIAS_MAILBOX));
    public String EMAIL_RECIPIENT_QA = ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.EMAIL_RECIPIENT_QA));

    @Before
    public void setupData() {
        ESendEmailModel requestMessageBody = new ESendEmailModel();
        requestUri = ConfigUtils.getProperty(ConfigUtils.ConfigKeys.BASE_URL_QA.toString()) +
                ConfigUtils.getProperty(ConfigUtils.ConfigKeys.MIDDLE_URL_E_MS_QA.toString()) +
                ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.END_URL_E_MS_QA));
        logger.info("Ecorr URL: " + " " + requestUri);

        requestBodyRo = requestMessageBody.createModel(EMAIL_MAILBOX, ALIAS_MAILBOX, EMAIL_RECIPIENT_QA, APP_ID, PHONE_NO_RO, EMAIL_SUBJECT_VALUE_RO);
        requestBodyES = requestMessageBody.createModel(EMAIL_MAILBOX, ALIAS_MAILBOX, EMAIL_RECIPIENT_QA, APP_ID, PHONE_NO_ES, EMAIL_SUBJECT_VALUE_ES);
        requestBodyPO = requestMessageBody.createModel(EMAIL_MAILBOX, ALIAS_MAILBOX, EMAIL_RECIPIENT_QA, APP_ID, PHONE_NO_PO, EMAIL_SUBJECT_VALUE_PO);
        requestBodyChinesse = requestMessageBody.createModel(EMAIL_MAILBOX, ALIAS_MAILBOX, EMAIL_RECIPIENT_QA, APP_ID, PHONE_NO_China, EMAIL_SUBJECT_VALUE_China);
        requestBodyDummy = requestMessageBody.createModel(EMAIL_MAILBOX, ALIAS_MAILBOX, EMAIL_RECIPIENT_QA, APP_ID_DUMMY, PHONE_NO_DUMMY, EMAIL_SUBJECT_VALUE_DUMMY);
        requestBodyWildCards = requestMessageBody.createModel(EMAIL_MAILBOX, ALIAS_MAILBOX, EMAIL_RECIPIENT_QA, APP_ID_WILDCARDS, PHONE_NO_WILDCARDS, EMAIL_SUBJECT_VALUE_WILDCARDS);

        connector = new RestAssuredConnector();
    }


    @Test
    public void postESendEmailValidationsRomanianNegativeTest() {
        RestAssuredConnector connector = new RestAssuredConnector();
        Map<String, String> headers = RestAssuredConnector.setHeaders(ConfigUtils.getProperty(ConfigUtils.ConfigKeys.TOKEN_E_MS.toString()));
        Response ecorrResponse = connector.postRequest(requestUri, headers, requestBodyRo);

        logger.info("requestBody:" + " " + requestBodyRo);
        logger.info("Response:" + " " + ecorrResponse.getBody().asString());
        assertEquals(ecorrResponse.getStatusCode(), 200);
    }
    @Test
    public void postESendEmailValidationsSpanishNegativeTest() {
        RestAssuredConnector connector = new RestAssuredConnector();
        Map<String, String> headers = RestAssuredConnector.setHeaders(ConfigUtils.getProperty(ConfigUtils.ConfigKeys.TOKEN_E_MS.toString()));
        Response ecorrResponse = connector.postRequest(requestUri, headers, requestBodyES);

        logger.info("requestBody:" + " " + requestBodyES);
        logger.info("Response:" + " " + ecorrResponse.getBody().asString());
        assertEquals(ecorrResponse.getStatusCode(), 200);
    }
    @Test
    public void postESendEmailValidationsPortugueseNegativeTest() {
        RestAssuredConnector connector = new RestAssuredConnector();
        Map<String, String> headers = RestAssuredConnector.setHeaders(ConfigUtils.getProperty(ConfigUtils.ConfigKeys.TOKEN_E_MS.toString()));
        Response ecorrResponse = connector.postRequest(requestUri, headers, requestBodyPO);

        logger.info("requestBody:" + " " + requestBodyPO);
        logger.info("Response:" + " " + ecorrResponse.getBody().asString());
        assertEquals(ecorrResponse.getStatusCode(), 200);
    }

    @Test
    public void postESendEmailValidationsChinesseNegativeTest() {
        RestAssuredConnector connector = new RestAssuredConnector();
        Map<String, String> headers = RestAssuredConnector.setHeaders(ConfigUtils.getProperty(ConfigUtils.ConfigKeys.TOKEN_E_MS.toString()));
        Response ecorrResponse = connector.postRequest(requestUri, headers, requestBodyChinesse);

        logger.info("requestBody:" + " " + requestBodyChinesse);
        logger.info("Response:" + " " + ecorrResponse.getBody().asString());
        assertEquals(ecorrResponse.getStatusCode(), 200);
    }

    @Test
    public void postESendEmailValidationsDummyNegativeTest() {
        RestAssuredConnector connector = new RestAssuredConnector();
        Map<String, String> headers = RestAssuredConnector.setHeaders(ConfigUtils.getProperty(ConfigUtils.ConfigKeys.TOKEN_E_MS.toString()));
        Response ecorrResponse = connector.postRequest(requestUri, headers, requestBodyDummy);

        logger.info("requestBody:" + " " + requestBodyDummy);
        logger.info("Response:" + " " + ecorrResponse.getBody().asString());
        assertEquals(ecorrResponse.getStatusCode(), 200);
    }

    @Test
    public void postESendEmailValidationsWildcardsNegativeTest() {
        RestAssuredConnector connector = new RestAssuredConnector();
        Map<String, String> headers = RestAssuredConnector.setHeaders(ConfigUtils.getProperty(ConfigUtils.ConfigKeys.TOKEN_E_MS.toString()));
        Response ecorrResponse = connector.postRequest(requestUri, headers, requestBodyWildCards);

        logger.info("requestBody:" + " " + requestBodyWildCards);
        logger.info("Response:" + " " + ecorrResponse.getBody().asString());
        assertEquals(ecorrResponse.getStatusCode(), 200);
    }
}
