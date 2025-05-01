package auto.mailboxmonitoring.micro_consumer;

import io.restassured.response.Response;
import auto.kafka.KafkaConsumerSetup;
import auto.restfulapis.ESendEmailModel;
import auto.utils.connector.RestAssuredConnector;
import auto.utils.general.ConfigUtils;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.json.JSONObject;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Map;
import java.util.Properties;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class POSTESendEmailConsumerValidationsNegativeTest {
    protected static final Logger logger = LoggerFactory.getLogger(POSTESendEmailConsumerValidationsNegativeTest.class);
    private String requestUri;
    public String requestBody;
    public String requestBodyRO;
    RestAssuredConnector connector;
    public String APP_ID = "DUMMY";
    public String PHONE_NO = "DUMMY";
    public String PHONE_NO_RO = "+40723456789";
    String pattern = "mm-dd-yyyy";
    SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);

    public String currentDate = dateFormat.format(new Date());
    String UNIQUE_GROUP_ID = "automation_doc_events_consumer" + currentDate;
    public String EMAIL_SUBJECT_VALUE = "Automated email sent with success" + " " + currentDate;
    public String EMAIL_SUBJECT_VALUE_RO = "Acesta este un email automat" + " " + currentDate;
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
        requestBodyRO = requestMessageBody.createModel(EMAIL_MAILBOX, ALIAS_MAILBOX, EMAIL_RECIPIENT_QA, APP_ID, PHONE_NO_RO, EMAIL_SUBJECT_VALUE_RO);

        connector = new RestAssuredConnector();
    }

    @Test
    public void postESendEmailConsumerValidationsNegativeTest() {
        RestAssuredConnector connector = new RestAssuredConnector();
        Map<String, String> headers = RestAssuredConnector.setHeaders(ConfigUtils.getProperty(ConfigUtils.ConfigKeys.TOKEN_E_MS.toString()));
        Response EResponse = connector.postRequest(requestUri, headers, requestBody);

        String ERsp = EResponse.getBody().asString();
        JSONObject EObj = new JSONObject(ERsp);
        String correlationIdMs = EObj.getString("correlationId");

        logger.info("requestUri:" + " " + requestUri);
        logger.info("requestBody:" + " " + requestBody);
        logger.info("Response:" + " " + EResponse.getBody().asString());
        assertEquals(EResponse.getStatusCode(), 200);

        KafkaConsumerSetup kafkaConsumerSetup = new KafkaConsumerSetup();
        logger.info("Adding properties to Kafka Consumer...");
        Properties propsConsumer = kafkaConsumerSetup.setConsumerProps(ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.BOOTSTREP_SERVER_QA)), UNIQUE_GROUP_ID, ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.AUTH_INFO_MAILBOX)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.SASL_CONFIG_MAILBOX)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.TRUST_STORE_LOCATION)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.SCHEMA_REGISTRY)));

        logger.info("Initializing and subscribing to the topic");
        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(propsConsumer);
        consumer.subscribe(Arrays.asList(ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.TOPIC_MAILBOX_CONSUMER))));
        ConsumerRecords<String, GenericRecord> records = consumer.poll(1000);
        for (ConsumerRecord<String, GenericRecord> record : records) {
            GenericRecord avroRecordConsum = record.value();
            logger.info("Current  record value: " + record.value().toString());
            String eventCorrelationIdConsumer = avroRecordConsum.get("eventHeader.eventCorrelationId").toString();
            assertThat(correlationIdMs).isNotEqualTo(eventCorrelationIdConsumer);
            consumer.close();
        }
    }

    @Test
    public void postESendEmailConsumerValidationsNOtUSNegativeTest() {
        RestAssuredConnector connector = new RestAssuredConnector();
        Map<String, String> headers = RestAssuredConnector.setHeaders(ConfigUtils.getProperty(ConfigUtils.ConfigKeys.TOKEN_E_MS.toString()));
        Response EResponse = connector.postRequest(requestUri, headers, requestBodyRO);

        String ERsp = EResponse.getBody().asString();
        JSONObject EObj = new JSONObject(ERsp);
        String correlationIdMs = EObj.getString("correlationId");

        logger.info("requestUri:" + " " + requestUri);
        logger.info("requestBody:" + " " + requestBodyRO);
        logger.info("Response:" + " " + EResponse.getBody().asString());
        assertEquals(EResponse.getStatusCode(), 200);

        KafkaConsumerSetup kafkaConsumerSetup = new KafkaConsumerSetup();
        logger.info("Adding properties to Kafka Consumer...");
        Properties propsConsumer = kafkaConsumerSetup.setConsumerProps(ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.BOOTSTREP_SERVER_QA)), UNIQUE_GROUP_ID, ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.AUTH_INFO_MAILBOX)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.SASL_CONFIG_MAILBOX)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.TRUST_STORE_LOCATION)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.SCHEMA_REGISTRY)));

        logger.info("Initializing and subscribing to the topic");
        KafkaConsumer<String, GenericRecord> consumer = new KafkaConsumer<>(propsConsumer);
        consumer.subscribe(Arrays.asList(ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.TOPIC_MAILBOX_CONSUMER))));
        ConsumerRecords<String, GenericRecord> records = consumer.poll(1000);
        for (ConsumerRecord<String, GenericRecord> record : records) {
            GenericRecord avroRecordConsum = record.value();
            logger.info("Current  record value: " + record.value().toString());
            String eventCorrelationIdConsumer = avroRecordConsum.get("eventHeader.eventCorrelationId").toString();
            assertThat(correlationIdMs).isNotEqualTo(eventCorrelationIdConsumer);
            consumer.close();
        }
    }
}



