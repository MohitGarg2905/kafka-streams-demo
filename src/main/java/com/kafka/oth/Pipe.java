package com.kafka.oth;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Predicate;

import java.util.concurrent.CountDownLatch;

public class Pipe {

    private static final String INPUT_TOPIC = "streams-plaintext-input";
    private static final String OUTPUT_TOPIC = "streams-pipe-output";

    static void createStream(final StreamsBuilder builder) {
        final KStream<String, String> source = builder.stream(INPUT_TOPIC);
        KStream<String, String> onlyPositives = source.filter(
                new Predicate<String, String>() {
                    @Override
                    public boolean test(String key, String value) {
                        System.out.println(key);
                        return value.length()>0;
                    }
                });
        onlyPositives.to(OUTPUT_TOPIC);
    }

    public static void main(String[] args){
        final StreamsBuilder builder = new StreamsBuilder();
        createStream(builder);
        final Topology topology = builder.build();
        System.out.println(topology.describe());
        final KafkaStreams streams = new KafkaStreams(topology, PropertiesUtils.getProperties("streams-pipe"));

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
