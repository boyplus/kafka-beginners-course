package io.thanaphon.demos;

import io.thanaphon.kafka.KafkaConfigurationBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemo {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am a consumer");
        String groupId = "my-java-application";
        String topic = "demo_java";

        // Create producer properties
        Properties properties = KafkaConfigurationBuilder.build();

        // Create consumer config
        properties.put("key.deserializer", StringDeserializer.class.getName());
        properties.put("value.deserializer", StringDeserializer.class.getName());

        properties.put("group.id", groupId);

        // none -> if we don't have any existing consumer group, then we fail. Which mean we must set the consumer group before we start application
        // earliest -> read from the beginning from the topic
        // latest -> read new message that is being produced
        properties.put("auto.offset.reset", "earliest");

        // Create a consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer(properties);

        // Subscribe to a topic
        consumer.subscribe(Arrays.asList(topic));

        // Poll for data
        while(true) {
            log.info("Polling");

            // .pool() -> how long we will wait to receive the data
            // If there is data ti be returned right away, this will be completed in no time as soon as the data is received
            // If kafka does not have any data for us, we are waiting for one second to receive data from kafka
            // The purpose is to not overloaded Kafka
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for(ConsumerRecord<String, String> record : records) {
                log.info("Key : " + record.key() + ", Value "+ record.value());
                log.info("Partition : " + record.partition() + ", Offset "+ record.offset());
            }
        }

        // When we start the consumer at the first time, the consumer group is created
        // And this consumer demo is joining the group, and it consumes the message at the beginning
        // And it also commits the offset on behalf of consumer group

        // Which means if we stop the consumer and then start it again
        // -> we rejoin the group, and it will consume from the latest offset, so it does not consume the previous message
    }
}
