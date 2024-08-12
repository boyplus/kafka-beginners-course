package io.thanaphon.wikimedia.kafka;

import javax.naming.ConfigurationException;
import java.io.IOException;

public class KafkaConfigurationLoader {
    public static KafkaConfiguration load() throws ConfigurationException {
        PropertyFileReader reader = new PropertyFileReader();
        try {
            String server = reader.getPropValues("kafka_server");
            String username = reader.getPropValues("kafka_username");
            String password = reader.getPropValues("kafka_password");
            return new KafkaConfiguration(server, username, password);
        } catch (IOException e) {
            throw new ConfigurationException();
        }
    }
}
