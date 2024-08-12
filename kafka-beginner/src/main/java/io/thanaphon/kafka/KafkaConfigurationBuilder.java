package io.thanaphon.kafka;

import java.util.Properties;

public class KafkaConfigurationBuilder {
    public static Properties build() {
        try {
            Properties properties = new Properties();
            KafkaConfiguration config = KafkaConfigurationLoader.load();
            properties.put("bootstrap.servers", config.server);
            properties.put("sasl.mechanism", "SCRAM-SHA-256");
            properties.put("security.protocol", "SASL_SSL");
            properties.put("sasl.jaas.config", String.format("org.apache.kafka.common.security.scram.ScramLoginModule required username=\"%s\" password=\"%s\";", config.username, config.password));
            return properties;
        } catch (Exception e) {
            return new Properties();
        }
    }
}
