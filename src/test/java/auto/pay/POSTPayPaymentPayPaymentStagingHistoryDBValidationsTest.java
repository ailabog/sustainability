package auto.Pay;

import io.restassured.response.Response;
import auto.db.DBConnections;
import auto.restfulapis.PayPaymentModel;
import auto.utils.general.ConfigUtils;
import auto.utils.connector.RestAssuredConnector;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Map;
import java.util.Random;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

public class POSTPayPaymentPayPaymentStagingHistoryDBValidationsTest {
    static Random random = new Random();
    int randomInt = random.nextInt(100000000) + 1;
    String AGREEMENT_KEY = randomInt + "|||" + "ISIQ";
    protected static final Logger logger = LoggerFactory.getLogger(POSTPayPaymentPayPaymentStagingHistoryDBValidationsTest.class);
    public String EMAIL = "";
    public String BANK_ACCOUNT_TYPE = "CHKG";
    public String GUID = String.valueOf(UUID.randomUUID());
    public int ACC_NO = random.nextInt(100000000) + 1;
    public long AMOUNT = ;
    public int ROUNTING_NO = random.nextInt(100000000) + 1;
    public String RECEIVABLE_TYPE = "";
    public String TRANS_TYPE = "";
    public String PAYMENT_METHOD = "";
    public String PAYOR_NAME = "";
    public String requestUri;
    public String requestBody;
    PayPaymentModel responseMessage;
    RestAssuredConnector connector;
    DBConnections dbConn;
    public String QUERY = "SELECT * FROM Pay.payment_staging_history where payment_staging_key = ?";

    @Before
    public void setupData() {
        PayPaymentModel requestMessageBody = new PayPaymentModel();
        dbConn = new DBConnections();
        requestUri = ConfigUtils.getProperty(ConfigUtils.ConfigKeys.BASE_URL_QA.toString()) +
                ConfigUtils.getProperty(ConfigUtils.ConfigKeys.MIDDLE_URL_PAYMENT_QA.toString()) +
                ConfigUtils.getProperty(ConfigUtils.ConfigKeys.END_URL_PAYMENT_QA.toString());
        requestBody = requestMessageBody.createModel(EMAIL, BANK_ACCOUNT_TYPE, GUID, ACC_NO, AMOUNT, ROUNTING_NO, RECEIVABLE_TYPE, TRANS_TYPE, AGREEMENT_KEY, PAYMENT_METHOD, PAYOR_NAME);
        responseMessage = new PayPaymentModel();
        connector = new RestAssuredConnector();
    }

    @Test
    public void postPaymentDBValidationsStagingHistoryHappyTest() throws SQLException {
        RestAssuredConnector connector = new RestAssuredConnector();
        Map<String, String> headers = RestAssuredConnector.setHeaders(ConfigUtils.getProperty(ConfigUtils.ConfigKeys.TOKEN_Pay_PAYMENT.toString()));
        Response PayResponse = connector.postRequest(requestUri, headers, requestBody);

        logger.info("requestUri:" + " " + requestUri);
        logger.info("requestBody:" + " " + requestBody);
        logger.info("Response:" + " " + PayResponse.getBody().asString());
        assertEquals(PayResponse.getStatusCode(), 200);
        int paymentKey = responseMessage.savePaymentKey(PayResponse);
        ResultSet rs = dbConn.dbConn(ConfigUtils.getProperty(ConfigUtils.ConfigKeys.Pay_DB_URL_QA.name()), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.Pay_DB_USERNAME_QA)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.Pay_DB_PASSWORD_QA)), QUERY, paymentKey);

        if (rs.next()) {
            int paymentStagingHistoryKeyDB = rs.getInt("payment_staging_history_key");
            int paymentStagingKeyDB = rs.getInt("payment_staging_key");
            int policyMasterKeyDB = rs.getInt("policy_master_key");
            String accountNumberDB = rs.getString("account_number");
            String routingNumberDB = rs.getString("routing_number");
            long paymentAmountDB = rs.getLong("payment_amount");
            String payorEmailAddressDB = rs.getString("payor_email_address");
            String creatorIdDB = rs.getString("creator_id");
            int paymentConfirmationDB = rs.getInt("payment_confirmation");
            String billingNoDB = rs.getString("billing_no");
            String submitGUIDDB = rs.getString("submit_guid");
            String submitstatusDB = rs.getString("submit_status");
            String rowProcessDateTimeDB = rs.getString("row_process_datetime");
            String customerMemberGuidDB = rs.getString("customer_member_guid");
            String payorfullNameDB = rs.getString("payor_full_name");
            if (paymentStagingKeyDB == (paymentKey)) {
                assertEquals(paymentAmountDB, AMOUNT);
                assertEquals(paymentConfirmationDB, paymentKey);
                logger.info("DB Validations for payment_staging_history_key: " + paymentStagingHistoryKeyDB + "  " + "payment_staging_keypolicyMasterKeyDB:" + paymentStagingKeyDB + "  " + "policy master key: " + policyMasterKeyDB + "  " + "accountNumberDB:" + accountNumberDB + "  " + "paymentAmountDB no: " + paymentAmountDB + " " + "routingNumberDB: " + routingNumberDB + "  " + "payorEmailAddressDB: " + payorEmailAddressDB + " " + "accountNumberDB: " + accountNumberDB + "  " + " billingNoDB:" + billingNoDB + " " + "paymentConfirmationDB: " + paymentConfirmationDB + " " + "submitGUIDDB: " + submitGUIDDB + "passed successfully");
            } else {
                logger.info("No payment staging key was found into the db");
            }
            dbConn.closeConnection(rs.getStatement().getConnection());
        }
    }
}
