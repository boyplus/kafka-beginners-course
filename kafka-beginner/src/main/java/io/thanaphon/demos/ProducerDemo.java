package io.thanaphon.demos;

import io.thanaphon.kafka.KafkaConfigurationBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

public class ProducerDemo {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am a producer");

        // Create producer properties
        Properties properties = KafkaConfigurationBuilder.build();
        // Set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer(properties);

        // Create a producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world");

        // Send data -- asynchronous
        producer.send(producerRecord);

        // Tell the producer to send add data and block until done -- synchronous
        producer.flush();

        // Flush and close the producer
        producer.close();
    }
}
