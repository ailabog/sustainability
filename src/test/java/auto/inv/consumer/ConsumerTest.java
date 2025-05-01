package auto.inv.consumer;

import auto.kafka.KafkaConsumerSetup;
import auto.utils.general.ConfigUtils;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;

public class ConsumerTest {
    protected static final Logger logger = LoggerFactory.getLogger(ConsumerTest.class);
    String pattern = "mm-dd-yyyy";
    SimpleDateFormat dateFormat = new SimpleDateFormat(pattern);
    String currentDate = dateFormat.format(new Date());
    String UNIQUE_GROUP_ID = "automation_doc_events_consumer" + currentDate;

    @Test
    public void subscribeToConsumerTopicHappyTest() {
        KafkaConsumerSetup kafkaConsumerSetup = new KafkaConsumerSetup();
        Properties props = kafkaConsumerSetup.setConsumerProps(ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.BOOTSTREP_SERVER_QA)), UNIQUE_GROUP_ID, ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.AUTH_INFO_INV)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.SASL_CONFIG_INV)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.TRUST_STORE_LOCATION)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.SCHEMA_REGISTRY)));
        logger.info("Adding properties to Kafka Consumer...");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList(ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.TOPIC_INV_CONSUMER))));
        consumer.close();
    }
}
