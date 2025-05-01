package auto.inv.producer_db;

import auto.db.DBConnections;
import auto.kafka.KafkaProducerSetup;
import auto.utils.general.ConfigUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
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

public class ProducerAvroSchemaInvDocTypeDBValidationsNegativeTest {
    protected static final Logger logger = LoggerFactory.getLogger(ProducerAvroSchemaInvDocTypeDBValidationsNegativeTest.class);

    Date now = new Date();
    SimpleDateFormat date = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    String currentDateComplete = date.format(now);
    UUID generateUUID = UUID.randomUUID();
    String eventType = "";
    String eventSubtype = "";
    String evId = generateUUID.toString();
    String eventSourceDescription = "ECM P8 Document Publish";
    String eventSource = "";
    String documentType = "Inv";
    String businessArea = "";
    String batchNoPfx = "";
    String objectStore = "";
    String mimeType = "application/pdf";
    String systemAddedID = "";
    String transactionID = "";
    String primaryHoldingID = "111111";
    String adminSystem = "llll";
    String docClass = "";
    String GUID = '{' + generateUUID.toString() + '}';
    DBConnections dbConn;
    public String QUERY = "SELECT * FROM message WHERE JSON_EXTRACT(payload, \"$.content.sharedData.origEvReqId\")= ?";

    @Test
    public void sendKafkaMessageInvDocTypeNegativeTest() throws ExecutionException, InterruptedException, SQLException {
        KafkaProducerSetup kafkaProducerSetup = new KafkaProducerSetup();

        logger.info("Adding properties to Kafka Producer...");
        Properties props = kafkaProducerSetup.setProducerProps(ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.BOOTSTREP_SERVER_QA)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.AUTH_INFO_INV)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.SASL_CONFIG_INV)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.TRUST_STORE_LOCATION)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.SCHEMA_REGISTRY)));
        GenericRecord message = KafkaProducerSetup.produceMessage(eventType, eventSubtype, currentDateComplete, evId, eventSourceDescription, eventSource, documentType, businessArea, batchNoPfx, objectStore, mimeType, systemAddedID, docClass, GUID);

        logger.info("Preparing/Sending the object to Kafka Producer");
        ProducerRecord<Object, Object> record = new ProducerRecord<>(ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.TOPIC_NAME_PRODUCER)), null, message);
        KafkaProducer<Object, Object> producer = new KafkaProducer<>(props);
        RecordMetadata metadata = producer.send(record).get();
        producer.flush();
        producer.close();

        logger.info("Message sent successfully to topic" + metadata.topic() + " " +
                "partition: " + metadata.partition() + " " + "offset: " + metadata.offset());
        dbConn = new DBConnections();
        ResultSet rs = dbConn.dbConn(ConfigUtils.getProperty(ConfigUtils.ConfigKeys.E_DB_URL_QA.name()), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.E_DB_USERNAME_QA)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.E_DB_PASSWORD_QA)), QUERY, evId);

        if(!rs.next()) {
            logger.info("There is no message in E DB");
        } else {
            logger.info("There is something wrong and there is  a message in E DB");
        }
        dbConn.closeConnection(rs.getStatement().getConnection());
    }
}