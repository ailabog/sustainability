package auto.inv.producer_consumer_micros_db;

import io.restassured.response.Response;
import auto.db.DBConnections;
import auto.kafka.KafkaConsumerSetup;
import auto.kafka.KafkaProducerSetup;
import auto.utils.connector.RestAssuredConnector;
import auto.utils.general.ConfigUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.assertj.core.api.AssertionsForClassTypes;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

public class ProducerConsumerAgrMsDBValidationsHappyTest {
    protected static final Logger logger = LoggerFactory.getLogger(ProducerConsumerAgrMsDBValidationsHappyTest.class);
    Date now = new Date();
    SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String currentDateComplete = date.format(now);
    UUID generateUUID = UUID.randomUUID();
    String eventType = "";
    String eventSubtype = "";
    String evId = generateUUID.toString();
    String evSrcDesc = "";
    String eventSource = "";
    String documentType = "";
    String businessArea = "";
    String batchNoPfx = "";
    String objectStore = "";
    String mimeType = "application/pdf";
    String systemAddedID = "";
    String transactionID = "";
    String primaryHId = "";
    String adminSystem = "";
    String docClass = "";
    String GUID = '{' + generateUUID.toString() + '}';
    String pattern = "mm-dd-yyyy";
    SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
    String currentDate = dateFormat.format(new Date());
    String UNIQUE_GROUP_ID = "automation_doc_events_consumer" + currentDate;
    public RestAssuredConnector connector;
    DBConnections dbConn;
    private String reqUriAgrCst;
    private final String agreement_key = "11111111";
    private final String agreement_key_end = "LOL";
    public String QUERY = "SELECT * FROM message WHERE type = \"push\" AND source = \"SOURCE\" AND JSON_EXTRACT(payload, \"$.triggers.onsuccess\") = \"inv-push-android\" AND  JSON_EXTRACT(payload, \"$.content.sharedData.origEvId\")= ?";

    @Before
    public void setup() {
        reqUriAgrCst = ConfigUtils.getProperty(ConfigUtils.ConfigKeys.BASE_URL_KONG_QA.toString()) +
                ConfigUtils.getProperty(ConfigUtils.ConfigKeys.MIDDLE_URL_AGREEMENT_QA.toString()) + agreement_key + "%7C%7C%7C" + agreement_key_end;
        connector = new RestAssuredConnector();
        dbConn = new DBConnections();
    }

    @Test
    public void produceMessageConsumerAgreementMsDBValidationsTest() throws ExecutionException, InterruptedException, SQLException {
        KafkaProducerSetup kafkaProducerSetup = new KafkaProducerSetup();

        logger.info("Adding properties to Kafka Producer...");
        Properties propsProducer = kafkaProducerSetup.setProducerProps(ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.BOOTSTREP_SERVER_QA)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.AUTH_INFO_INV)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.SASL_CONFIG_INV)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.TRUST_STORE_LOCATION)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.SCHEMA_REGISTRY)));
        GenericRecord message = KafkaProducerSetup.produceMessage(eventType, eventSubtype, currentDateComplete, evId, evSrcDesc, eventSource, documentType, businessArea, batchNoPfx, objectStore, mimeType, systemAddedID, docClass, GUID);

        logger.info("Preparing/Sending the object to Kafka Producer");
        ProducerRecord<Object, Object> recordProducer = new ProducerRecord<>(ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.TOPIC_NAME_PRODUCER)), null, message);
        KafkaProducer<Object, Object> producer = new KafkaProducer<>(propsProducer);
        RecordMetadata metadata = producer.send(recordProducer).get();
        producer.flush();
        producer.close();

        logger.info("Message sent successfully to topic" + metadata.topic() + " " +
                "partition: " + metadata.partition() + " " + "offset: " + metadata.offset());
        KafkaConsumerSetup kafkaConsumerSetup = new KafkaConsumerSetup();

        logger.info("Adding properties to Kafka Consumer...");
        Properties propsConsumer = kafkaConsumerSetup.setConsumerProps(ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.BOOTSTREP_SERVER_QA)), UNIQUE_GROUP_ID, ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.AUTH_INFO_INV)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.SASL_CONFIG_INV)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.TRUST_STORE_LOCATION)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.SCHEMA_REGISTRY)));

        logger.info("Initializing and subscribing to the topic");
        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(propsConsumer);
        consumer.subscribe(Arrays.asList(ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.TOPIC_INV_CONSUMER))));
        ConsumerRecords<String, GenericRecord> records = consumer.poll(1000);
        for (ConsumerRecord<String, GenericRecord> record : records) {
            GenericRecord avroRecordConsum = record.value();
            logger.info("Current  record value: " + record.value().toString());
            String evIdConsumer = avroRecordConsum.get("eventHeader.evId").toString();

            if (evId.equals(evIdConsumer)) {
                assertThat(avroRecordConsum.get("eventHeader.eventType").toString()).isEqualTo(eventType);
                assertThat(avroRecordConsum.get("eventHeader.eventSubtype").toString()).isEqualTo(eventSubtype);
                assertThat(avroRecordConsum.get("eventHeader.eventDateTime").toString()).isEqualTo(currentDateComplete);
                assertThat(avroRecordConsum.get("eventHeader.eventGeneratedDateTime").toString()).isEqualTo(currentDateComplete);
                assertThat(avroRecordConsum.get("eventHeader.evId").toString()).isEqualTo(evId);
                assertThat(avroRecordConsum.get("eventHeader.eventRequestId").toString()).isEqualTo(evId);
                assertThat(avroRecordConsum.get("eventHeader.evSrcDesc").toString()).isEqualTo(evSrcDesc);
                assertThat(avroRecordConsum.get("eventBody.Document_Type").toString()).isEqualTo(documentType);
                assertThat(avroRecordConsum.get("eventBody.BusinessArea").toString()).isEqualTo(businessArea);
                assertThat(avroRecordConsum.get("eventBody.Batch_No_Pfx").toString()).isEqualTo(batchNoPfx);
                assertThat(avroRecordConsum.get("eventBody.ObjectStore").toString()).isEqualTo(objectStore);
                assertThat(avroRecordConsum.get("eventBody.MimeType").toString()).isEqualTo(mimeType);
                assertThat(avroRecordConsum.get("eventBody.SystemAddedID").toString()).isEqualTo(systemAddedID);
                assertThat(avroRecordConsum.get("eventBody.Transaction_ID").toString()).isEqualTo(transactionID);
                assertThat(avroRecordConsum.get("eventBody.Primary_Holding_ID1").toString()).isEqualTo(primaryHId);
                assertThat(avroRecordConsum.get("eventBody.AdminSystem").toString()).isEqualTo(adminSystem);
            }
            long offset = record.offset();
            logger.info("Consumed record at offset: " + offset);
        }
        consumer.close();
        logger.info("Validating the Consumer fields against the fields produced... with success, messages is sent on Consumer side");

        logger.info("Agreement customer call:");
        Map<String, String> headers = RestAssuredConnector.setHeaders(ConfigUtils.getProperty(ConfigUtils.ConfigKeys.TOKEN_AGREEMENT_CUSTOMER.toString()));
        Response agreementResponse = connector.getRequest(reqUriAgrCst, headers);

        logger.info("Extracting customer, agreement key, role type and member GUID");
        String responseBody_agreementKey = agreementResponse.getBody().asString();
        JSONObject jsonResponse = new JSONObject(responseBody_agreementKey);

        JSONArray agreements = jsonResponse.getJSONArray("agreements");
        JSONObject customer = agreements.getJSONObject(0).getJSONArray("agreementCustomers").getJSONObject(0);

        JSONArray agreementsArray = jsonResponse.getJSONArray("agreements");
        String agreementKeyMS = agreementsArray.getJSONObject(0).getString("agreementKey");

        String roleType = customer.getString("roleType");
        String memberGUIDAgreementCustomer = customer.getString("memberGUID");
        logger.info(memberGUIDAgreementCustomer);
        AssertionsForClassTypes.assertThat(roleType).isEqualTo("OWNR");
        AssertionsForClassTypes.assertThat(agreementKeyMS).isEqualTo(agreement_key + "|||" + agreement_key_end);
        logger.info("Validating the DB payload:");
        ResultSet rs = dbConn.dbConn(ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.E_DB_URL_QA)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.ECORR_DB_USERNAME_QA)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.ECORR_DB_PASSWORD_QA)), QUERY, evId);

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
            String originalEventInitiatorDescription = sharedData.getString("originalEventInitiatorDescription");
            String originalApplicationId = sharedData.getString("originalApplicationId");
            String policyNumber = sharedData.getString("policyNumber");
            String originalEventRequestID = sharedData.getString("originalEventRequestID");
            String originalevSrcDesc = sharedData.getString("originalevSrcDesc");
            String originalEventSource = sharedData.getString("originalEventSource");
            String originalEventInitiator = sharedData.getString("originalEventInitiator");
            String originalEventDateTime = sharedData.getString("originalEventDateTime");

            String fileName = jsonPayload.getJSONArray("items").getJSONObject(0).getJSONObject("source").getJSONObject("parameters").getString("fileName");
            String sound = jsonPayload.getJSONArray("items").getJSONObject(0).getJSONObject("source").getJSONObject("data").getString("sound");
            String templateType = jsonPayload.getJSONArray("items").getJSONObject(0).getString("templateType");
            String sourceType = jsonPayload.getJSONArray("items").getJSONObject(0).getJSONObject("source").getString("sourceType");
            JSONArray onsuccess = jsonPayload.getJSONObject("triggers").getJSONArray("onsuccess");
            String DbmessageType = jsonPayload.getString("messageType");

            if (origEvId.equals(evId)) {

                assertThat(originalApplicationId).isEqualTo("DC-ORCH");
                assertThat(overwriteMessageType).isEqualTo("push");
                assertThat(originalEventInitiatorDescription).isEqualTo(evSrcDesc);
                assertThat(policyNumber).isEqualTo(agreement_key);
                assertThat(originalevSrcDesc).isEqualTo(evSrcDesc);
                assertThat(originalEventSource).isEqualTo("comm-orch");
                assertThat(originalEventRequestID).isEqualTo(evId);
                assertThat(originalEventDateTime).isEqualTo(currentDateComplete);
                assertThat(originalEventInitiator).isEqualTo(applicationId);
                assertThat(fileName).isEqualTo("inv-push");
                        assertThat(templateType).isEqualTo("");
                assertThat(sourceType).isEqualTo("");
                assertThat(DbmessageType).isEqualTo("push");
            } else {
                logger.info("Something must be really wrong with this data");
            }
        }
        dbConn.closeConnection(rs.getStatement().getConnection());
    }
}


