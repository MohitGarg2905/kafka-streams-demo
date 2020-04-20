package com.kafka.streams;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.stereotype.Component;

import static com.kafka.commons.Topics.*;

@Component
public class StreamsMerge extends StreamsBase{

    protected void createStream(final StreamsBuilder builder) {
        KStream<String, String> source1 = builder.stream(getInputTopic());
        KStream<String, String> source2 = builder.stream(MERGE_INPUT_TOPIC_1);

        KStream<String, String> output = source1.merge(source2);

        output.to(MERGE_OUTPUT_TOPIC);
    }

    protected String getInputTopic() {
        return MERGE_INPUT_TOPIC_2;
    }
}
