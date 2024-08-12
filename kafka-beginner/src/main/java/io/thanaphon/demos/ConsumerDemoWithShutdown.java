package io.thanaphon.demos;

import io.thanaphon.kafka.KafkaConfigurationBuilder;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoWithShutdown {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());
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

        // Get a reference to the main thread
        final Thread mainThread = Thread.currentThread();

        // Adding a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run(){
                log.info("Detected a shutdownm let's exit by calling consumer.wakeup()...");
                consumer.wakeup();

                // Join the main thread to allow the execution of the code in the main thread
                try {
                    mainThread.join();
                } catch(InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            // Subscribe to a topic
            consumer.subscribe(Arrays.asList(topic));

            // Poll for data
            while(true) {
                log.info("Polling");

                // Poll -> will throw WakeupException
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for(ConsumerRecord<String, String> record : records) {
                    log.info("Key : " + record.key() + ", Value "+ record.value());
                    log.info("Partition : " + record.partition() + ", Offset "+ record.offset());
                }
            }
        } catch(WakeupException e) {
            log.info("Consumer is starting to shut down");
        } catch(Exception e) {
            log.error("Unexpected exception in the consumer", e);
        } finally {
            consumer.close(); // close the consumer, this will also commit offsets
            log.info("Consumer is now gracefully shutdown");
        }
    }
}
