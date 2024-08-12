package io.thanaphon.demos;

import io.thanaphon.kafka.KafkaConfigurationBuilder;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am a producer, produce message with callback");

        // Create producer properties
        Properties properties = KafkaConfigurationBuilder.build();

        // Set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // This is for learning purpose only, please don't use it in prod
        // properties.put("batch.size", "400");
        // properties.put("partitioner.class", RoundRobinPartitioner.class.getName());

        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer(properties);

        for(int j=0; j<10; j++) {
            for(int i=0; i < 30; i++) {
                // Create a producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world " + i);

                // Send data -- asynchronous
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        // execute every time a record successfully sent or an exception is thrown
                        if(e == null) {
                            // the record was successfully sent
                            log.info("Received new metadata \n" +
                                    "Topic: " + recordMetadata.topic() + "\n" +
                                    "Partition: " + recordMetadata.partition() + "\n" +
                                    "Offset: " + recordMetadata.offset() + "\n" +
                                    "Timestamp: " + recordMetadata.timestamp() + "\n"
                            );
                        } else {
                            log.error("Error while producing ", e);
                        }
                    }
                });
            }
            try {
                Thread.sleep(500);
            } catch(Exception e) {

            }
        }

        // Tell the producer to send add data and block until done -- synchronous
        producer.flush();

        // Flush and close the producer
        producer.close();
    }
}
