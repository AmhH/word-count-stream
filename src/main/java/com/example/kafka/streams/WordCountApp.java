package com.example.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {

    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "word-count-app");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        Topology topology = new WordCountApp().createTopology();

        KafkaStreams streams = new KafkaStreams(topology, config);
        streams.start();

        //Prints the topology
        System.out.println("********************************");
        System.out.println(streams.toString());

        //Shutdown hook to correctly / gracefully close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        // Update:
        // print the topology every 10 seconds for learning purposes
        while(true){
            streams.localThreadsMetadata().forEach(data -> System.out.println(data));
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    Topology createTopology() {
        StreamsBuilder builder = new StreamsBuilder();
        // 1 - stream from Kafka
        KStream<String, String> wordCountInput = builder.stream("word-count-input");

        KTable<String, Long> wordCount = wordCountInput
                            // 2 - map values to lowercase
                            .mapValues((ValueMapper<String, String>) String::toLowerCase)
                            // 3 - flat map values split by space
                            .flatMapValues(value -> Arrays.asList(value.split("\\W+")))
                            // 4 - select key to apply a key (we discard the old key)
                            .selectKey((key, value) -> value)
                            // 5 - group by key before aggregation
                            .groupByKey()
                            // 6 - count occurrences
                            .count(Materialized.as("Counts"));

        // 7 - to in order to write the results back to kafka
        wordCount.toStream().to("word-count-output", Produced.with(Serdes.String(), Serdes.Long()));
        return builder.build();
    }
}
