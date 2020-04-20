package com.kafka.oth;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Arrays;
import java.util.concurrent.CountDownLatch;

public class LineSplit {

    private static final String INPUT_TOPIC = "streams-plaintext-input";
    private static final String OUTPUT_TOPIC = "streams-wordcount-output";

    static void createStream(final StreamsBuilder builder) {
        KStream<String, String> source = builder.stream(INPUT_TOPIC);
        KStream<String, String> words = source.flatMapValues(value -> Arrays.asList(value.split("\\W+")));
        words.to(OUTPUT_TOPIC);
    }

    public static void main(String[] args){
        final StreamsBuilder builder = new StreamsBuilder();
        createStream(builder);
        final Topology topology = builder.build();
        System.out.println(topology.describe());
        final KafkaStreams streams = new KafkaStreams(topology, PropertiesUtils.getProperties("streams-line-split"));

        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
