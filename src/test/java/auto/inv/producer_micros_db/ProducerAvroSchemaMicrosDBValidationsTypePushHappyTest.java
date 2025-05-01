package auto.inv.producer_micros_db;

import io.restassured.response.Response;
import auto.db.DBConnections;
import auto.kafka.KafkaProducerSetup;
import auto.utils.connector.RestAssuredConnector;
import auto.utils.general.ConfigUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.junit.Assert.assertEquals;

public class ProducerAvroSchemaMicrosDBValidationsTypePushHappyTest {
    protected static final Logger logger = LoggerFactory.getLogger(ProducerAvroSchemaMicrosDBValidationsTypePushHappyTest.class);

    Date now = new Date();
    SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String currentDateComplete = date.format(now);
    UUID generateUUID = UUID.randomUUID();
    String eventType = "";
    String eventSubtype = "";
    String eventCorrelationId = generateUUID.toString();
    String eventSourceDescription = "  ";
    String eventSource = "";
    String documentType = "";
    String businessArea = "";
    String batchNoPfx = "";
    String objectStore = "";
    String mimeType = "application/pdf";
    String systemAddedID = "";
    String docClass = "";
    String GUID = '{' + generateUUID.toString() + '}';
    String applicationIdExpected = "";
    String messageType = "";
    DBConnections dbConn;
    private String requestUriAgreementCustomer;
    private String requestUriCustomerId;
    String expectedRole = "";
    public RestAssuredConnector connector;
    public Response agreementResponse;
    public Response customerIdResponse;
    public String memberGUIDCustomerId;
    public String QUERY = "SELECT * FROM message WHERE type = \"push\" AND source = \"SOURCE\" AND JSON_EXTRACT(payload, \"$.triggers.onsuccess\") = \"push-android\" AND  JSON_EXTRACT(payload, \"$.content.sharedData.origEvId\")= ?";
    private final String agr_key="1111111";
    private final String agr_key_end="LL";
    private String CUSTOMER_ID = "202c95cf-0147-dfgh-56gh-1286aa06455e";

    @Before
    public void setupData() {
        dbConn = new DBConnections();
        requestUriCustomerId = ConfigUtils.getProperty(ConfigUtils.ConfigKeys.BASE_URL_KONG_QA.toString()) +
                ConfigUtils.getProperty(ConfigUtils.ConfigKeys.MIDDLE_URL_CUSTOMERID_QA.toString()) + CUSTOMER_ID;

        requestUriAgreementCustomer = ConfigUtils.getProperty(ConfigUtils.ConfigKeys.BASE_URL_KONG_QA.toString()) +
                ConfigUtils.getProperty(ConfigUtils.ConfigKeys.MIDDLE_URL_AGREEMENT_QA.toString()) + agr_key + "%7C%7C%7C" + agr_key_end;

        connector = new RestAssuredConnector();
    }
    @Test
    public void sendKafkaMessageDbValidationsHappyTest() throws ExecutionException, InterruptedException, SQLException {
        KafkaProducerSetup kafkaProducerSetup = new KafkaProducerSetup();

        logger.info("Adding properties to Kafka Producer...");
        Properties props = kafkaProducerSetup.setProducerProps(ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.BOOTSTREP_SERVER_QA)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.AUTH_INFO_INV)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.SASL_CONFIG_INV)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.TRUST_STORE_LOCATION)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.SCHEMA_REGISTRY)));
        GenericRecord message = KafkaProducerSetup.produceMessage(eventType, eventSubtype, currentDateComplete, eventCorrelationId, eventSourceDescription, eventSource, documentType, businessArea, batchNoPfx, objectStore, mimeType, systemAddedID, docClass, GUID);

        logger.info("Preparing/Sending the object to Kafka Producer");
        ProducerRecord<Object, Object> record = new ProducerRecord<>(ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.TOPIC_NAME_PRODUCER)), null, message);
        KafkaProducer<Object, Object> producer = new KafkaProducer<>(props);
        RecordMetadata metadata = producer.send(record).get();
        producer.flush();
        producer.close();

        logger.info("Message sent successfully to topic" + metadata.topic() + " " +
                "partition: " + metadata.partition() + " " + "offset: " + metadata.offset());

        logger.info("Agreement customer call:");
        Map<String, String> headers = RestAssuredConnector.setHeaders(ConfigUtils.getProperty(ConfigUtils.ConfigKeys.TOKEN_AGREEMENT_CUSTOMER.toString()));
        Response agreementResponse = connector.getRequest(requestUriAgreementCustomer, headers);

        logger.info("Extracting customer, agreement key, role type and member GUID");
        String responseBody_agreementKey = agreementResponse.getBody().asString();
        JSONObject jsonResponse = new JSONObject(responseBody_agreementKey);

        JSONArray agreements = jsonResponse.getJSONArray("agreements");
        JSONObject customer = agreements.getJSONObject(0).getJSONArray("agreementCustomers").getJSONObject(0);

        JSONArray agreementsArray = jsonResponse.getJSONArray("agreements");
        String agreementKeyMS = agreementsArray.getJSONObject(0).getString("agreementKey");

        String roleType = customer.getString("roleType");

        Map<String, String> headersCustomer = RestAssuredConnector.setHeaders(ConfigUtils.getProperty(ConfigUtils.ConfigKeys.TOKEN_CUSTOMER_ID.toString()));
        Response customerIdResponse = connector.getRequest(requestUriCustomerId, headersCustomer);
        String responseBody_customerId = customerIdResponse.getBody().asString();
        JSONObject customerResponse = new JSONObject(responseBody_customerId);

        JSONArray customers = customerResponse.getJSONArray("customers");
        String memberGUIDCustomerId = customers.getJSONObject(0).getString("memberGUID");

        assertThat(roleType).isEqualTo(expectedRole);
        assertThat(agreementKeyMS).isEqualTo(agr_key + "|||" + agr_key_end);

        logger.info("Validating the DB payload:");
        ResultSet rs = dbConn.dbConn(ConfigUtils.getProperty(ConfigUtils.ConfigKeys.E_DB_URL_QA.name()), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.E_DB_USERNAME_QA)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.E_DB_PASSWORD_QA)), QUERY, eventCorrelationId);

        if (rs.next()) {
            String type = rs.getString("type");
            String source = rs.getString("source");
            String payload = rs.getString("payload");
            String received_date = rs.getString("received_date");

            logger.info("Validating the DB payload:");

            JSONObject jsonPayload = new JSONObject(payload);
            JSONObject sharedData = jsonPayload.getJSONObject("content").getJSONObject("sharedData");

            String origEvId = sharedData.getString("origEvId");
            String applicationId = jsonPayload.getString("applicationId");
            String overwriteMessageType = sharedData.getString("overwriteMessageType");
            String userId = sharedData.getString("userId");
            String origEvInitDesc = sharedData.getString("origEvInitDesc");
            String origAppId = sharedData.getString("origAppId");
            String policyNumber = sharedData.getString("policyNumber");
            String origEvReqId = sharedData.getString("origEvReqId");
            String origEvSrcDesc = sharedData.getString("origEvSrcDesc");
            String originalEventSource = sharedData.getString("originalEventSource");
            String originalEventInitiator = sharedData.getString("originalEventInitiator");
            String originalEventDateTime = sharedData.getString("originalEventDateTime");

            String fileName = jsonPayload.getJSONArray("items").getJSONObject(0).getJSONObject("source").getJSONObject("parameters").getString("fileName");
            String sound = jsonPayload.getJSONArray("items").getJSONObject(0).getJSONObject("source").getJSONObject("data").getString("sound");
            String templateType = jsonPayload.getJSONArray("items").getJSONObject(0).getString("templateType");
            String sourceType = jsonPayload.getJSONArray("items").getJSONObject(0).getJSONObject("source").getString("sourceType");
            JSONArray onsuccess = jsonPayload.getJSONObject("triggers").getJSONArray("onsuccess");
            String DbmessageType = jsonPayload.getString("messageType");

            if (origEvId.equals(eventCorrelationId)) {

                assertThat(origAppId).isEqualTo(applicationIdExpected);
                assertThat(overwriteMessageType).isEqualTo(messageType);
                assertThat(origEvInitDesc).isEqualTo(eventSourceDescription);
                assertThat(policyNumber).isEqualTo(agr_key);
                assertThat(origEvSrcDesc).isEqualTo(eventSourceDescription);
                assertThat(originalEventSource).isEqualTo("comm-orch");
                assertThat(origEvReqId).isEqualTo(eventCorrelationId);
                assertThat(originalEventDateTime).isEqualTo(currentDateComplete);
                assertThat(userId).isEqualTo(memberGUIDCustomerId);
                assertThat(originalEventInitiator).isEqualTo(applicationId);
                assertThat(fileName).isEqualTo("");
                assertThat(templateType).isEqualTo("");
                assertThat(sourceType).isEqualTo("");
                assertThat(DbmessageType).isEqualTo(messageType);
            } else {
                logger.info("Something must be really wrong with this data");
            }
        }
        dbConn.closeConnection(rs.getStatement().getConnection());
    }
}

