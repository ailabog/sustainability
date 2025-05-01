package auto.inv.producer_db;

import auto.db.DBConnections;
import auto.kafka.KafkaProducerSetup;
import auto.utils.general.ConfigUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

public class ProducerAvroSchemaDBValidationsTypePHappyTest {
    protected static final Logger logger = LoggerFactory.getLogger(ProducerAvroSchemaDBValidationsTypePHappyTest.class);

    Date now = new Date();
    SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String currentDateComplete = date.format(now);
    UUID generateUUID = UUID.randomUUID();
    String eventType = "";";
    String eventCorrelationId = generateUUID.toString();
    String eventSourceDescription = "";
    String eventSource = "";
    String documentType = "";
    String businessArea = "";
    String batchNoPfx = "";
    String objectStore = "";
    String mimeType = "application/pdf";
    String systemAddedID = "";
    String transactionID = "";
    String primaryHoldingID = "111111";
    String primaryHoldingIDCompose = "11111";
    String adminSystem = "";
    String docClass = "Annuities_Records";
    String GUID = '{' + generateUUID.toString() + '}';
    String applicationIdExpected = "";
    String messageType = "";
    DBConnections dbConn;
    public String QUERY = "SELECT * FROM message WHERE type = \"push\" AND source = \"SOURCE\" AND JSON_EXTRACT(payload, \"$.triggers.onsuccess\") = \"push-android\" AND  JSON_EXTRACT(payload, \"$.content.sharedData.origEvId\")= ?";

    @Test
    public void produceSimpleAgreementKeyMessageDbValidationsHappyTest() throws ExecutionException, InterruptedException, SQLException {
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

        logger.info("Validating the DB payload:");
        dbConn = new DBConnections();
        ResultSet rs = dbConn.dbConn(ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.E_DB_URL_QA)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.E_DB_USERNAME_QA)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.E_DB_PASSWORD_QA)), QUERY, eventCorrelationId);

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
            String originalEventInitiatorDescription = sharedData.getString("originalEventInitiatorDescription");
            String originalApplicationId = sharedData.getString("originalApplicationId");
            String policyNumber = sharedData.getString("policyNumber");
            String originalEventRequestID = sharedData.getString("originalEventRequestID");
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

                assertThat(originalApplicationId).isEqualTo(applicationIdExpected);
                assertThat(overwriteMessageType).isEqualTo(messageType);
                assertThat(originalEventInitiatorDescription).isEqualTo(eventSourceDescription);
                assertThat(policyNumber).isEqualTo(primaryHoldingID);
                assertThat(origEvSrcDesc).isEqualTo(eventSourceDescription);
                assertThat(originalEventSource).isEqualTo("comm-orch");
                assertThat(originalEventRequestID).isEqualTo(eventCorrelationId);
                assertThat(originalEventDateTime).isEqualTo(currentDateComplete);
                assertThat(originalEventInitiator).isEqualTo(applicationId);
                assertThat(fileName).isEqualTo("");
                assertThat(templateType).isEqualTo("thymeleaf");
                assertThat(sourceType).isEqualTo("");
                assertThat(DbmessageType).isEqualTo(messageType);
            } else {
                logger.info("Something must be really wrong with this data");
            }
        }
        dbConn.closeConnection(rs.getStatement().getConnection());
    }
    @Test
    public void produceComposeAgreementKeyMessageDbValidationsHappyTest() throws ExecutionException, InterruptedException, SQLException {
        KafkaProducerSetup kafkaProducerSetup = new KafkaProducerSetup();

        logger.info("Adding properties to Kafka Producer...");
        Properties props = kafkaProducerSetup.setProducerProps(ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.BOOTSTREP_SERVER_QA)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.AUTH_INFO_INV)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.SASL_CONFIG_INV)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.TRUST_STORE_LOCATION)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.SCHEMA_REGISTRY)));
        GenericRecord message = KafkaProducerSetup.produceMessageComposeAgreementKey(eventType, eventSubtype, currentDateComplete, eventCorrelationId, eventSourceDescription, eventSource, documentType, businessArea, batchNoPfx, objectStore, mimeType, systemAddedID, docClass, GUID);

        logger.info("Preparing/Sending the object to Kafka Producer");
        ProducerRecord<Object, Object> record = new ProducerRecord<>(ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.TOPIC_NAME_PRODUCER)), null, message);
        KafkaProducer<Object, Object> producer = new KafkaProducer<>(props);
        RecordMetadata metadata = producer.send(record).get();
        producer.flush();
        producer.close();

        logger.info("Message sent successfully to topic" + metadata.topic() + " " +
                "partition: " + metadata.partition() + " " + "offset: " + metadata.offset());

        logger.info("Validating the DB payload:");
        dbConn = new DBConnections();
        ResultSet rs = dbConn.dbConn(ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.E_DB_URL_QA)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.E_DB_USERNAME_QA)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.E_DB_PASSWORD_QA)), QUERY, eventCorrelationId);

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
            String originalEventInitiatorDescription = sharedData.getString("originalEventInitiatorDescription");
            String originalApplicationId = sharedData.getString("originalApplicationId");
            String policyNumber = sharedData.getString("policyNumber");
            String originalEventRequestID = sharedData.getString("originalEventRequestID");
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

                assertThat(originalApplicationId).isEqualTo(applicationIdExpected);
                assertThat(overwriteMessageType).isEqualTo(messageType);
                assertThat(originalEventInitiatorDescription).isEqualTo(eventSourceDescription);
                assertThat(origEvSrcDesc).isEqualTo(eventSourceDescription);
                assertThat(originalEventSource).isEqualTo("comm-orch");
                assertThat(originalEventRequestID).isEqualTo(eventCorrelationId);
                assertThat(originalEventDateTime).isEqualTo(currentDateComplete);
                assertThat(policyNumber).isEqualTo(primaryHoldingIDCompose);
                assertThat(originalEventInitiator).isEqualTo(applicationId);
                assertThat(fileName).isEqualTo("");
                assertThat(templateType).isEqualTo("thymeleaf");
                assertThat(sourceType).isEqualTo("");
                assertThat(DbmessageType).isEqualTo(messageType);
            } else {
                logger.info("Something must be really wrong with this data");
            }
        }
        dbConn.closeConnection(rs.getStatement().getConnection());
    }
}

