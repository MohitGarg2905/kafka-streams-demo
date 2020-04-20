package com.kafka.streams;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.stereotype.Component;

import static com.kafka.commons.Topics.*;

@Component
public class StreamsFilter extends StreamsBase{

    protected void createStream(final StreamsBuilder builder) {
        KStream<String, String> source = builder.stream(getInputTopic());
        KStream<String, String> peeked = source.peek((key, value) -> System.out.println("Received on "+getInputTopic()+" Key: "+key+", value: "+value));
        peeked.filter(
                (key, value) -> value.contains("allow")
        ).to(FILTERS_OUTPUT_TOPIC);
        peeked.filterNot(
                (key, value) -> value.contains("allow")
        ).to(FILTERS_NOT_OUTPUT_TOPIC);
    }

    protected String getInputTopic() {
        return FILTERS_INPUT_TOPIC;
    }
}
