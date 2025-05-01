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

public class ProducerAvroSchemaDBValidationsTypePAndroidNegativeTest {
    protected static final Logger logger = LoggerFactory.getLogger(ProducerAvroSchemaDBValidationsTypePAndroidNegativeTest.class);

    Date now = new Date();
    SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String currentDateComplete = date.format(now);
    UUID generateUUID = UUID.randomUUID();
    String eventType = "";
    String eventSubtype = "";
    String eventCorrelationId = generateUUID.toString();
    String eventSourceDescription = "";
    String eventSource = "";
    String documentType = "DUMMY";
    String businessArea = "";
    String batchNoPfx = "ANN";
    ;
    String objectStore = "";
    String mimeType = "application/pdf";
    String systemAddedID = "";
    String primaryHoldingID = "111111111111";
    String docClass = "";
    String GUID = '{' + generateUUID.toString() + '}';
    String applicationIdExpected = "";
    String messageType = "";
    DBConnections dbConn;
    String QUERY = "SELECT ident, type, format, source, received_date, status, payload FROM message WHERE type = \"push\" AND source = \"SOURCE\" AND  JSON_EXTRACT(payload, \"$.content.sharedData.appIdType\")=\"androidAppId\" AND  JSON_EXTRACT(payload, \"$.content.sharedData.origEvReqId\")=?";

    @Test
    public void produceMessageDBValidationsTypePAndroidNegativeTest() throws ExecutionException, InterruptedException, SQLException {
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

        if (!rs.next()) {
            logger.info("There is no message in E DB");
        } else {
            logger.info("There is something wrong and there is  a message in E DB");
        }
        dbConn.closeConnection(rs.getStatement().getConnection());
    }
}

