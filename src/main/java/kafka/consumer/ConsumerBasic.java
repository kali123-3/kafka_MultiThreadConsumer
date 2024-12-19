package kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerBasic {
    public static void main(String[] args) {
        // Set up properties for the Kafka consumer
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092"); // Kafka broker
        //Understanding GroupId. In Kafka, a consumer group is a collection of consumer instances that collaborate to consume messages from one or more topics.
        properties.put("group.id", "test-group"); // Consumer group ID
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // Create the Kafka consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties);

        // Subscribe to a topic  named we should had this topic and already created...
        consumer.subscribe(Arrays.asList("test-autre"));

        try {
            while (true) {
                // Poll for new records
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Consumed message: key = %s, value = %s, offset = %d, partition = %d%n",
                            record.key(), record.value(), record.offset(), record.partition());
                }
            }
        } finally {
            consumer.close();
        }


    }
}
