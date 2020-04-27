package com.dante.kafka.streams;

import com.dante.kafka.commons.Topics;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.stereotype.Component;

@Component
public class StreamsMap extends StreamsBase{

    protected void createStream(final StreamsBuilder builder) {
        KStream<String, String> source = builder.stream(getInputTopic());
        KStream<String, String> output = source.mapValues(
                value -> value.toUpperCase()
        );
        output.to(Topics.MAP_OUTPUT_TOPIC);
    }

    protected String getInputTopic() {
        return Topics.MAP_INPUT_TOPIC;
    }
}
