package kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerKafkaMultiThread {

    public static void main(String[] args) {
        // Set up properties for the Kafka consumer
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092"); // Kafka broker
        //Understanding GroupId. In Kafka, a consumer group is a collection of consumer instances that collaborate to consume messages from one or more topics.
        properties.put("group.id", "test-group"); // Consumer group ID
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        BasicConsumLoop basicConsumLoop1 = new BasicConsumLoop(properties, Arrays.asList("test-autre")) {
            @Override
            public void process(ConsumerRecord record) {
                System.out.printf("Consumed 1 message: key = %s, value = %s, offset = %d, partition = %d%n",
                        record.key(), record.value(), record.offset(), record.partition());

            }
        };

        BasicConsumLoop basicConsumLoop2 = new BasicConsumLoop(properties, Arrays.asList("test-autre")) {
            @Override
            public void process(ConsumerRecord record) {
                System.out.printf("Consumed 2 message: key = %s, value = %s, offset = %d, partition = %d%n",
                        record.key(), record.value(), record.offset(), record.partition());

            }
        };

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.execute(basicConsumLoop1);
        executorService.execute(basicConsumLoop2);






    }

}
