package auto.inv.producer_db;

import auto.db.DBConnections;
import auto.kafka.KafkaProducerSetup;
import auto.utils.general.ConfigUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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

public class ProducerAvroSchemaDBValidationsTypePKafkaHappyTest {
    protected static final Logger logger = LoggerFactory.getLogger(ProducerAvroSchemaDBValidationsTypePKafkaHappyTest.class);

    Date now = new Date();
    SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String currentDateComplete = date.format(now);
    UUID generateUUID = UUID.randomUUID();
    String eventType = "";
    String eventSubtype = "";
    String eventCorrelationId = generateUUID.toString();
    String eventSourceDescription = "";
    String eventSource = "";
    String documentType = "";
    String businessArea = "";
    String batchNoPfx = "";
    ;    String objectStore = "";
    String mimeType = "application/pdf";
    String systemAddedID = "";
    String primaryHoldingID = "11111111111";
    String docClass = "";
    String GUID = '{' + generateUUID.toString() + '}';
    String applicationIdExpected = "";
    String messageType = "kafka";
    DBConnections dbConn;
    String messageTypeExpected = "";
    String QUERY = "SELECT ident, type, format, source, received_date, status, payload FROM message WHERE type = \"kafka\" AND source = \"SOURCE\" AND  JSON_EXTRACT(payload, \"$.content.sharedData.appIdType\")=\"androidAppId\" AND  JSON_EXTRACT(payload, \"$.content.sharedData.OrigEvReqId\")=?";

    @Test
    public void produceMessageDBValidationsTypePKafkaHappyTest() throws ExecutionException, InterruptedException, SQLException {
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
            assertThat(type).isEqualTo(messageType);
            assertThat(source).isEqualTo(applicationIdExpected);

            logger.info("Validating the DB payload:");
            JSONObject jsonPayload = new JSONObject(payload);
            JSONObject sharedData = jsonPayload.getJSONObject("content").getJSONObject("sharedData");

            String applicationId = sharedData.getString("applicationId");
            String overwriteMessageType = sharedData.getString("overwriteMessageType");
            String origEvInitDesc = sharedData.getString("origEvInitDesc");
            String originalMessageType = sharedData.getString("originalMessageType");
            String triggeredTemplate = sharedData.getString("triggeredTemplate");
            String originalApplicationId = sharedData.getString("originalApplicationId");
            String no = sharedData.getString("no");
            String OrigEvReqId = sharedData.getString("OrigEvReqId");
            //String userId = sharedData.getString("userId")
            String originalEventSourceDescription = sharedData.getString("originalEventSourceDescription");
            String originalEventSource = sharedData.getString("originalEventSource");
            String originalEventInitiator = sharedData.getString("originalEventInitiator");
            String originalEventDateTime = sharedData.getString("originalEventDateTime");

            String fileName = jsonPayload.getJSONArray("items").getJSONObject(0).getJSONObject("source").getJSONObject("parameters").getString("fileName");
            String templateType = jsonPayload.getJSONArray("items").getJSONObject(0).getString("templateType");
            String sourceType = jsonPayload.getJSONArray("items").getJSONObject(0).getJSONObject("source").getString("sourceType");

            String messageType = jsonPayload.getString("messageType");
            String messageFormat = jsonPayload.getString("messageFormat");

            if (OrigEvReqId.equals(eventCorrelationId)) {

                assertThat(applicationId).isEqualTo(applicationIdExpected);
                assertThat(triggeredTemplate).isEqualTo("");
                assertThat(originalMessageType).isEqualTo(messageTypeExpected);
                assertThat(overwriteMessageType).isEqualTo(messageType);
                assertThat(origEvInitDesc).isEqualTo("");
                assertThat(no).isEqualTo(primaryHoldingID);
                //softly.assertThat(userId).isEqualTo(userId)
                assertThat(originalEventSourceDescription).isEqualTo("");
                assertThat(originalEventSource).isEqualTo("");
                assertThat(OrigEvReqId).isEqualTo(eventCorrelationId);
                assertThat(originalEventDateTime).isEqualTo(currentDateComplete);
                assertThat(no).isEqualTo(primaryHoldingID);
                assertThat(originalEventInitiator).isEqualTo("");
                assertThat(fileName).isEqualTo("customer-push-notification-success-event-info");
                assertThat(templateType).isEqualTo("thymeleaf");
                assertThat(messageType).isEqualTo("kafka");
                assertThat(messageFormat).isEqualTo("json");
            } else {
                logger.info("Something must be really wrong with this data");
            }
        }
        dbConn.closeConnection(rs.getStatement().getConnection());
    }
}

