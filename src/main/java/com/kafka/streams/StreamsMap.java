package com.kafka.streams;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;

import static com.kafka.commons.Topics.*;

@Component
public class StreamsMap extends StreamsBase{

    protected void createStream(final StreamsBuilder builder) {
        KStream<String, String> source = builder.stream(getInputTopic());
        KStream<String, String> output = source.mapValues(
                value -> value.toUpperCase()
        );
        output.to(MAP_OUTPUT_TOPIC);
    }

    protected String getInputTopic() {
        return MAP_INPUT_TOPIC;
    }
}
