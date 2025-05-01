package auto.inv.producer_consumer;

import auto.kafka.KafkaConsumerSetup;
import auto.kafka.KafkaProducerSetup;
import auto.utils.general.ConfigUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;

public class ProducerAvroSchemaConsumerValidationsHappyTest {
    protected static final Logger logger = LoggerFactory.getLogger(ProducerAvroSchemaConsumerValidationsHappyTest.class);
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
    String objectStore = "";
    String mimeType = "application/pdf";
    String systemAddedID = "";
    String transactionID = "";
    String primaryHoldingID = "";
    String adminSystem = "";
    String docClass = "Annuities_Records";
    String GUID = '{' + generateUUID.toString() + '}';
    String pattern = "mm-dd-yyyy";
    SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
    String currentDate = dateFormat.format(new Date());
    String UNIQUE_GROUP_ID = "automation_doc_events_consumer" + currentDate;

    @Test
    public void producerMessageConsumerValidationsHappyTest() throws ExecutionException, InterruptedException {
        KafkaProducerSetup kafkaProducerSetup = new KafkaProducerSetup();

        logger.info("Adding properties to Kafka Producer...");
        Properties propsProducer = kafkaProducerSetup.setProducerProps(ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.BOOTSTREP_SERVER_QA)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.AUTH_INFO_INV)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.SASL_CONFIG_INV)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.TRUST_STORE_LOCATION)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.SCHEMA_REGISTRY)));
        GenericRecord message = KafkaProducerSetup.produceMessage(eventType, eventSubtype, currentDateComplete, eventCorrelationId, eventSourceDescription, eventSource, documentType, businessArea, batchNoPfx, objectStore, mimeType, systemAddedID, docClass, GUID);

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
            String eventCorrelationIdConsumer = avroRecordConsum.get("eventHeader.eventCorrelationId").toString();

            if(eventCorrelationId.equals(eventCorrelationIdConsumer)) {
                assertThat(avroRecordConsum.get("eventHeader.eventType").toString()).isEqualTo(eventType);
                assertThat(avroRecordConsum.get("eventHeader.eventSubtype").toString()).isEqualTo(eventSubtype);
                assertThat(avroRecordConsum.get("eventHeader.eventDateTime").toString()).isEqualTo(currentDateComplete);
                assertThat(avroRecordConsum.get("eventHeader.eventGeneratedDateTime").toString()).isEqualTo(currentDateComplete);
                assertThat(avroRecordConsum.get("eventHeader.eventCorrelationId").toString()).isEqualTo(eventCorrelationId);
                assertThat(avroRecordConsum.get("eventHeader.eventRequestId").toString()).isEqualTo(eventCorrelationId);
                assertThat(avroRecordConsum.get("eventHeader.eventSourceDescription").toString()).isEqualTo(eventSourceDescription);
                assertThat(avroRecordConsum.get("eventBody.Document_Type").toString()).isEqualTo(documentType);
                assertThat(avroRecordConsum.get("eventBody.BusinessArea").toString()).isEqualTo(businessArea);
                assertThat(avroRecordConsum.get("eventBody.Batch_No_Pfx").toString()).isEqualTo(batchNoPfx);
                assertThat(avroRecordConsum.get("eventBody.ObjectStore").toString()).isEqualTo(objectStore);
                assertThat(avroRecordConsum.get("eventBody.MimeType").toString()).isEqualTo(mimeType);
                assertThat(avroRecordConsum.get("eventBody.SystemAddedID").toString()).isEqualTo(systemAddedID);
                assertThat(avroRecordConsum.get("eventBody.Transaction_ID").toString()).isEqualTo(transactionID);
                assertThat(avroRecordConsum.get("eventBody.Primary_Holding_ID1").toString()).isEqualTo(primaryHoldingID);
                assertThat(avroRecordConsum.get("eventBody.AdminSystem").toString()).isEqualTo(adminSystem);
            }
            long offset = record.offset();
            logger.info("Consumed record at offset: " + offset);
        }
        consumer.close();
    }
}

