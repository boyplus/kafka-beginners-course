package io.thanaphon.wikimedia.demos;

import com.launchdarkly.eventsource.EventSource;
import io.thanaphon.wikimedia.kafka.KafkaConfigurationBuilder;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import com.launchdarkly.eventsource.EventHandler;

import java.net.URI;
import java.util.Properties;

public class WikimediaChangesProducer {
    public static void main(String[] args) {
        // Create producer properties
        Properties properties = KafkaConfigurationBuilder.build();
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer(properties);

        String topic = "wikimedia.recentchange";

        // TODO
        EventHandler eventHandler = null;
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource  =builder.build();

        // Start the producer in another thread
        eventSource.start();
    }
}
