package com.dante.kafka.streams;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.stereotype.Component;

import static com.dante.kafka.commons.Topics.*;

@Component
public class StreamsSelectKey extends StreamsBase{

    protected void createStream(final StreamsBuilder builder) {
        KStream<String, String> source = builder.stream(getInputTopic());

        KStream<String, String> peeked = source.peek((key, value) -> System.out.println("Received on "+getInputTopic()+" Key: "+key+", value: "+value));

        KStream<String, String> output = peeked.selectKey((key, value) -> getInputTopic()+"_"+value.toUpperCase());

        output.to(SELECT_KEY_OUTPUT_TOPIC);
    }

    protected String getInputTopic() {
        return SELECT_KEY_INPUT_TOPIC;
    }
}
