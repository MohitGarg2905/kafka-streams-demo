package com.dante.kafka.streams;

import com.dante.kafka.commons.Topics;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.stereotype.Component;

@Component
public class StreamsMerge extends StreamsBase{

    protected void createStream(final StreamsBuilder builder) {
        KStream<String, String> source1 = builder.stream(getInputTopic());
        KStream<String, String> source2 = builder.stream(Topics.MERGE_INPUT_TOPIC_1);

        KStream<String, String> output = source1.merge(source2);

        output.to(Topics.MERGE_OUTPUT_TOPIC);
    }

    protected String getInputTopic() {
        return Topics.MERGE_INPUT_TOPIC_2;
    }
}
