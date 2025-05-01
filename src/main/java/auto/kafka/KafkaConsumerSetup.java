package auto.kafka;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class KafkaConsumerSetup {
    public Properties setConsumerProps(String bootstrapServers, String uniqueGroupID, String authInfo, String jaasConfig, String trustStoreLocation, String schemaRegistryUrl) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, uniqueGroupID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put("security.protocol", "SASL_SSL");
        props.put("sasl.mechanism", "PLAIN");
        props.put("basic.auth.credentials.source", "USER_INFO");
        props.put("basic.auth.user.info", authInfo);
        props.put("sasl.jaas.config", jaasConfig);
        props.put("ssl.truststore.location", trustStoreLocation);
        props.put("ssl.truststore.password", "changeit");
        props.put("schema.registry.url", schemaRegistryUrl);
        props.put("schema.registry.ssl.truststore.location", trustStoreLocation);
        props.put("schema.registry.ssl.truststore.password", "changeit");
        props.put("auto.offset.reset", "latest");
        return props;
    }
}
