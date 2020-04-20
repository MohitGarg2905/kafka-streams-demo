package com.kafka.streams;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.stereotype.Component;

import java.util.LinkedList;
import java.util.List;

import static com.kafka.commons.Topics.*;

@Component
public class StreamsForeach extends StreamsBase{

    protected void createStream(final StreamsBuilder builder) {
        KStream<String, String> source = builder.stream(getInputTopic());
        source.foreach(
                (key,value) -> System.out.println(value)
        );
    }

    protected String getInputTopic() {
        return FOREACH_INPUT_TOPIC;
    }
}
