package io.thanaphon.demos;

import io.thanaphon.kafka.KafkaConfigurationBuilder;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());
    public static void main(String[] args) {
        log.info("I am a producer, produce message with callback");

        // Create producer properties
        Properties properties = KafkaConfigurationBuilder.build();

        // Set producer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // Create the Producer
        KafkaProducer<String, String> producer = new KafkaProducer(properties);

        // The same key goes to the same partition
        for(int j=0; j < 2; j++) {
            for(int i=0; i < 10; i++) {
                String topic = "demo_java";
                String key  ="id_" + i;
                String value  ="hello world " + i;
                // Create a producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                // Send data -- asynchronous
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        // execute every time a record successfully sent or an exception is thrown
                        if(e == null) {
                            // the record was successfully sent
                            log.info("Key: " + key + " | " + "Partition: " + recordMetadata.partition() + "\n");
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
