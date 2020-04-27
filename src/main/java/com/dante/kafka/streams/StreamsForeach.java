package com.dante.kafka.streams;

import com.dante.kafka.commons.Topics;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.stereotype.Component;

@Component
public class StreamsForeach extends StreamsBase{

    protected void createStream(final StreamsBuilder builder) {
        KStream<String, String> source = builder.stream(getInputTopic());
        source.foreach(
                (key,value) -> System.out.println(value)
        );
    }

    protected String getInputTopic() {
        return Topics.FOREACH_INPUT_TOPIC;
    }
}
