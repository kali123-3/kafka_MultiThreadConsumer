package kafka.Kstream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KGroupedStream;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.UUID;

public class Basique {
    private static final String INPUT_CONFIG = "string-input";
    private static final String OUTPUT_CONFIG = "string-output";

    public static void main(String[] args) {
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9092"); // Kafka broker
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, UUID.randomUUID().toString()); // Consumer group ID
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        // Create
        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String,String> source = builder.stream(INPUT_CONFIG);
       //filter
        source = source.filter((key,value) ->{return key.length()>5 ? true : false;});
        //source.mapValues((v)->v.toUpperCase());
        // interet de spli
        source.flatMap((k,v)->{
            //split value into multiple k,value0..............
            String[] tokens = v.split(" ");
            List<KeyValue<String,String>> result = new ArrayList<>(tokens.length);
            for(int i=0;i<tokens.length;i++){
                result.add(new KeyValue<>(k,tokens[i]));
            }
            return result;
        });

        /*
        Operation pour traitmeent
         */

        source.foreach((k,v)->{
            System.out.println(v);
        });
/*
Operation for deboggage
        source.peek((k,v)->{
            System.out.println(k + v);
        });
*/

//2 - OPERATION STATEFUUL POUR ACCEDER AUX AUTRE RECODRDES.

       KGroupedStream t = source.groupByKey();
       KTable p = t.reduce((a, b)-> String.valueOf(a)+String.valueOf(b));
       KStream s = p.toStream();
       s.to("total");








        source.to(OUTPUT_CONFIG, Produced.with(Serdes.String(),Serdes.String()));

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(),properties);
        kafkaStreams.start();

    }
}
