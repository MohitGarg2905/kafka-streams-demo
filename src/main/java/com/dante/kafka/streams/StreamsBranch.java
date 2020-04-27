package com.dante.kafka.streams;

import com.dante.kafka.commons.Topics;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.stereotype.Component;

@Component
public class StreamsBranch extends StreamsBase{

    private static final String OUTPUT_TOPIC_B = "streams-branch-output-b";
    private static final String OUTPUT_TOPIC_OTHER = "streams-branch-output-other";

    protected void createStream(final StreamsBuilder builder) {
        KStream<String, String> source = builder.stream(getInputTopic());
        KStream<String, String> peeked = source.peek((key, value) -> System.out.println("Received on "+getInputTopic()+" Key: "+key+", value: "+value));
        KStream<String, String>[] words = peeked.branch(
                (key, value) -> key.startsWith("filter"),
                (key, value) -> key.startsWith("flat"),
                (key, value) -> key.startsWith("for"),
                (key, value) -> key.startsWith("group"),
                (key, value) -> key.startsWith("map"),
                (key, value) -> key.startsWith("merge1"),
                (key, value) -> key.startsWith("merge2"),
                (key, value) -> key.startsWith("select"),
                (key, value) -> key.startsWith("count")
        );

        words[0].to(Topics.FILTERS_INPUT_TOPIC);
        words[1].to(Topics.FLAT_MAP_INPUT_TOPIC);
        words[2].to(Topics.FOREACH_INPUT_TOPIC);
        words[3].to(Topics.GROUPBY_INPUT_TOPIC);
        words[4].to(Topics.MAP_INPUT_TOPIC);
        words[5].to(Topics.MERGE_INPUT_TOPIC_1);
        words[6].to(Topics.MERGE_INPUT_TOPIC_2);
        words[7].to(Topics.SELECT_KEY_INPUT_TOPIC);
        words[8].to(Topics.COUNT_INPUT_TOPIC);
    }

    protected String getInputTopic() {
        return Topics.BRANCH_INPUT_TOPIC;
    }
}
