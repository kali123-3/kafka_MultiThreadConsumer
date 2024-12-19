package kafka.producer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.protocol.types.Field;

import java.util.Properties;

public class ProducerBasic {
    public static void main(String[] args) {
        // Set up properties for the Kafka consumer
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092"); // Kafka broker
        properties.put("group.id", "test-group"); // Consumer group ID
        //the same now we have serializer not a desrializer conver java object to Table
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("acks", "all");//Pour etre sur que mon messa
        Producer<String, String> producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 10; i++) {
            producer.send(new ProducerRecord<String, String>("kafka-test", Integer.toString(i), Integer.toString(i)));
        }

        //if i finish i have to close producer when i will not use anynore
        producer.close();
    }
}
