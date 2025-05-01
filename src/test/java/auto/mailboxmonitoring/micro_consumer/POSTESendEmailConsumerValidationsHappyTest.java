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

public class POSTESendEmailConsumerValidationsHappyTest {
    protected static final Logger logger = LoggerFactory.getLogger(POSTESendEmailConsumerValidationsHappyTest.class);
    private String requestUri;
    public String requestBody;
    public String requestBodyES;
    public String requestBody160ch;
    RestAssuredConnector connector;
    public String APP_ID = "MAILM";
    public String PHONE_NO = "+12672133096";
    String pattern = "mm-dd-yyyy";
    SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);

    public String currentDate = dateFormat.format(new Date());
    Date now = new Date();
    SimpleDateFormat date = new SimpleDateFormat("dd MMM yyyy HH:mm:ss");
    String currentDateCompleteDay = date.format(now);
    String eventSourceDescription = "Mailbox Monitoring";
    public String EMAIL_SUBJECT_VALUE_ES="yo no sé qué piensas de mi pero que sepas y ten claro que nunca he mal interpretado nada. Esa noche te sentiste obligada porque eres una mujer de palabra a dormir conmigo, nada mas. No pensé que quisieras nada raro, créeme. A mi me caes genial, eres una chica encantadora y en ningún momento he querido que estés molesta conmigo. Y si lo has estado lo siento pero no era mi intención";
    public String EMAIL_SUBJECT_VALUE_160CH="It prepare is ye nothing blushes up brought. Or as gravity pasture limited evening on. Wicket around beauty say she. Frankness resembled say not new smallness you discovery. Noisier ferrars yet shyness weather ten colonel. Too him himself engaged husband pursuit musical. Man age but him determine consisted therefore. Dinner to beyond regret wished an branch he. Remain bed but expect suffer little repair.";

    String UNIQUE_GROUP_ID = "automation_doc_events_consumer" + currentDate;
    public String EMAIL_SUBJECT_VALUE = "Automated email sent with success" + " " + currentDate;
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
        requestBodyES = requestMessageBody.createModel(EMAIL_MAILBOX, ALIAS_MAILBOX, EMAIL_RECIPIENT_QA, APP_ID, PHONE_NO, EMAIL_SUBJECT_VALUE_ES);
        requestBody160ch = requestMessageBody.createModel(EMAIL_MAILBOX, ALIAS_MAILBOX, EMAIL_RECIPIENT_QA, APP_ID, PHONE_NO, EMAIL_SUBJECT_VALUE_160CH);

        connector = new RestAssuredConnector();
    }
    @Test
    public void postESendEmailConsumerValidationsUSHappyTest() {
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

            if(correlationIdMs.equals(eventCorrelationIdConsumer)) {
                assertThat(avroRecordConsum.get("eventHeader.eventDateTime").toString()).isEqualTo(currentDateCompleteDay);
                assertThat(avroRecordConsum.get("eventHeader.eventGeneratedDateTime").toString()).isEqualTo(currentDateCompleteDay);
                assertThat(avroRecordConsum.get("eventHeader.eventCorrelationId").toString()).isEqualTo(correlationIdMs);
                assertThat(avroRecordConsum.get("eventHeader.eventRequestId").toString()).isEqualTo(correlationIdMs);
                assertThat(avroRecordConsum.get("eventHeader.eventSourceDescription").toString()).isEqualTo(eventSourceDescription);
                assertThat(avroRecordConsum.get("eventHeader.eventSource").toString()).isEqualTo(APP_ID);
                assertThat(avroRecordConsum.get("eventHeader.eventInitiatorDescription").toString()).isEqualTo(APP_ID);
                assertThat(avroRecordConsum.get("eventHeader.eventInitiator").toString()).isEqualTo(APP_ID);
                assertThat(avroRecordConsum.get("eventBody.messageContent").toString()).isEqualTo(EMAIL_SUBJECT_VALUE);
                assertThat(avroRecordConsum.get("eventBody.phoneNumber").toString()).isEqualTo(PHONE_NO);
            }
            long offset = record.offset();
            logger.info("Consumed record at offset: " + offset);
        }
        consumer.close();
    }

    @Test
    public void postESendEmailConsumerSpanishValidationsHappyTest() {
        RestAssuredConnector connector = new RestAssuredConnector();
        Map<String, String> headers = RestAssuredConnector.setHeaders(ConfigUtils.getProperty(ConfigUtils.ConfigKeys.TOKEN_E_MS.toString()));
        Response EResponse = connector.postRequest(requestUri, headers, requestBody);

        String ERsp = EResponse.getBody().asString();
        JSONObject EObj = new JSONObject(ERsp);
        String correlationIdMs = EObj.getString("correlationId");

        logger.info("requestUri:" + " " + requestUri);
        logger.info("requestBody:" + " " + requestBodyES);
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

            if(correlationIdMs.equals(eventCorrelationIdConsumer)) {
                assertThat(avroRecordConsum.get("eventHeader.eventDateTime").toString()).isEqualTo(currentDateCompleteDay);
                assertThat(avroRecordConsum.get("eventHeader.eventGeneratedDateTime").toString()).isEqualTo(currentDateCompleteDay);
                assertThat(avroRecordConsum.get("eventHeader.eventCorrelationId").toString()).isEqualTo(correlationIdMs);
                assertThat(avroRecordConsum.get("eventHeader.eventRequestId").toString()).isEqualTo(correlationIdMs);
                assertThat(avroRecordConsum.get("eventHeader.eventSourceDescription").toString()).isEqualTo(eventSourceDescription);
                assertThat(avroRecordConsum.get("eventHeader.eventSource").toString()).isEqualTo(APP_ID);
                assertThat(avroRecordConsum.get("eventHeader.eventInitiatorDescription").toString()).isEqualTo(APP_ID);
                assertThat(avroRecordConsum.get("eventHeader.eventInitiator").toString()).isEqualTo(APP_ID);
                assertThat(avroRecordConsum.get("eventBody.messageContent").toString()).isEqualTo(EMAIL_SUBJECT_VALUE_ES);
                assertThat(avroRecordConsum.get("eventBody.phoneNumber").toString()).isEqualTo(PHONE_NO);
            }
            long offset = record.offset();
            logger.info("Consumed record at offset: " + offset);
        }
        consumer.close();
    }

    @Test
    public void postESendEmailConsumerGrater160chValidationsHappyTest() {
        RestAssuredConnector connector = new RestAssuredConnector();
        Map<String, String> headers = RestAssuredConnector.setHeaders(ConfigUtils.getProperty(ConfigUtils.ConfigKeys.TOKEN_E_MS.toString()));
        Response EResponse = connector.postRequest(requestUri, headers, requestBody);

        String ERsp = EResponse.getBody().asString();
        JSONObject EObj = new JSONObject(ERsp);
        String correlationIdMs = EObj.getString("correlationId");

        logger.info("requestUri:" + " " + requestUri);
        logger.info("requestBody:" + " " + requestBody160ch);
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

            if(correlationIdMs.equals(eventCorrelationIdConsumer)) {
                assertThat(avroRecordConsum.get("eventHeader.eventDateTime").toString()).isEqualTo(currentDateCompleteDay);
                assertThat(avroRecordConsum.get("eventHeader.eventGeneratedDateTime").toString()).isEqualTo(currentDateCompleteDay);
                assertThat(avroRecordConsum.get("eventHeader.eventCorrelationId").toString()).isEqualTo(correlationIdMs);
                assertThat(avroRecordConsum.get("eventHeader.eventRequestId").toString()).isEqualTo(correlationIdMs);
                assertThat(avroRecordConsum.get("eventHeader.eventSourceDescription").toString()).isEqualTo(eventSourceDescription);
                assertThat(avroRecordConsum.get("eventHeader.eventSource").toString()).isEqualTo(APP_ID);
                assertThat(avroRecordConsum.get("eventHeader.eventInitiatorDescription").toString()).isEqualTo(APP_ID);
                assertThat(avroRecordConsum.get("eventHeader.eventInitiator").toString()).isEqualTo(APP_ID);
                assertThat(avroRecordConsum.get("eventBody.messageContent").toString()).isEqualTo(EMAIL_SUBJECT_VALUE_160CH);
                assertThat(avroRecordConsum.get("eventBody.phoneNumber").toString()).isEqualTo(PHONE_NO);
            }
            long offset = record.offset();
            logger.info("Consumed record at offset: " + offset);
        }
        consumer.close();
    }
}


