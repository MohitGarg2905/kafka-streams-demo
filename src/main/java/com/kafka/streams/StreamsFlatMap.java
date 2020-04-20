package com.kafka.streams;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;

import static com.kafka.commons.Topics.*;

@Component
public class StreamsFlatMap extends StreamsBase{

    protected void createStream(final StreamsBuilder builder) {
        KStream<String, String> source = builder.stream(getInputTopic());
        KStream<String, String> peeked = source.peek((key, value) -> System.out.println("Received on "+getInputTopic()+" Key: "+key+", value: "+value));
        KStream<String, String> output = peeked.flatMapValues(
                value -> {
                    List<String> result = new LinkedList<>();
                    result.add("Upper-" +value.toUpperCase());
                    result.add("Lower-" +value.toLowerCase());
                    return result;
                }
        );
        output.to(FLAT_MAP_OUTPUT_TOPIC);
    }

    protected String getInputTopic() {
        return FLAT_MAP_INPUT_TOPIC;
    }
}
