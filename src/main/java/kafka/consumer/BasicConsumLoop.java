package kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class BasicConsumLoop<T> implements Runnable {
    private final KafkaConsumer<T, T> consumer;
    private final List<String> topics;
    private final AtomicBoolean shutdown;

    public BasicConsumLoop(Properties config, List<String> topics) {
        this.consumer = new KafkaConsumer<T, T> (config);
        this.topics = topics;
        this.shutdown = new AtomicBoolean(false);
    }

    public abstract void process(ConsumerRecord<T,T> record);

    public void run() {
        try {
            consumer.subscribe(topics);
            while (!shutdown.get()) {
                // Poll for new records
                ConsumerRecords<T, T> records = consumer.poll(Duration.ofMillis(100));
                records.forEach(record -> process(record));
            }
        }finally{
            consumer.close();
        }
    }


}
