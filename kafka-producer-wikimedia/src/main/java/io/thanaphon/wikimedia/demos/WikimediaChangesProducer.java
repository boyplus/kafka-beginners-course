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
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {
    public static void main(String[] args) throws InterruptedException {
        // Create producer properties
        Properties properties = KafkaConfigurationBuilder.build();
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Create the producer
        KafkaProducer<String, String> producer = new KafkaProducer(properties);

        String topic = "wikimedia.recentchange";

        // TODO
        EventHandler eventHandler = new WikimediaChangeHandler(producer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";

        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource  =builder.build();

        // Start the producer in another thread
        eventSource.start();

        // we produce for 10 minutes and block the program ujntil then

        TimeUnit.MINUTES.sleep(10);
    }
}
