package auto.inv.producer;

import auto.kafka.KafkaProducerSetup;
import auto.utils.general.ConfigUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

public class DLQProducerSendNullMessageNegativeTest {
    protected static final Logger logger = LoggerFactory.getLogger(DLQProducerSendNullMessageNegativeTest.class);

    @Test
    public void sendDlqNullMessageNegativeTest() {
        KafkaProducerSetup kafkaProducerSetup = new KafkaProducerSetup();

        logger.info("Adding properties to Kafka Producer...");
        Properties props = kafkaProducerSetup.setProducerProps(ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.BOOTSTREP_SERVER_QA)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.AUTH_INFO_INV)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.SASL_CONFIG_INV)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.TRUST_STORE_LOCATION)), ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.SCHEMA_REGISTRY)));

        logger.info("Preparing/Sending the object to Kafka Producer");
        ProducerRecord<Object, Object> record = new ProducerRecord<>(ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.TOPIC_NAME_PRODUCER_DLQ)), null, null);
        KafkaProducer<Object, Object> producer = new KafkaProducer<>(props);

        producer.send(record);
        producer.flush();
        producer.close();

        logger.info("A new poison pill, null message was sent on topic:" + ConfigUtils.getProperty(String.valueOf(ConfigUtils.ConfigKeys.TOPIC_NAME_PRODUCER_DLQ)));
    }
}
