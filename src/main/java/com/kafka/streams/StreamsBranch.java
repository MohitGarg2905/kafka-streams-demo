package com.kafka.streams;

import com.kafka.oth.PropertiesUtils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;

import static com.kafka.commons.Topics.*;

@Component
public class StreamsBranch extends StreamsBase{

    private static final String OUTPUT_TOPIC_B = "streams-branch-output-b";
    private static final String OUTPUT_TOPIC_OTHER = "streams-branch-output-other";

    protected void createStream(final StreamsBuilder builder) {
        KStream<String, String> source = builder.stream(getInputTopic());
        KStream<String, String> peeked = source.peek((key, value) -> System.out.println("Received on "+getInputTopic()+" Key: "+key+", value: "+value));
        KStream<String, String>[] words = peeked.branch(
                (key, value) -> value.startsWith("filter"),
                (key, value) -> value.startsWith("flat"),
                (key, value) -> value.startsWith("for"),
                (key, value) -> value.startsWith("group"),
                (key, value) -> value.startsWith("map"),
                (key, value) -> value.startsWith("merge1"),
                (key, value) -> value.startsWith("merge2"),
                (key, value) -> value.startsWith("select"),
                (key, value) -> value.startsWith("count")

        );
        words[0].to(FILTERS_INPUT_TOPIC);
        words[1].to(FLAT_MAP_INPUT_TOPIC);
        words[2].to(FOREACH_INPUT_TOPIC);
        words[3].to(GROUPBY_INPUT_TOPIC);
        words[4].to(MAP_INPUT_TOPIC);
        words[5].to(MERGE_INPUT_TOPIC_1);
        words[6].to(MERGE_INPUT_TOPIC_2);
        words[7].to(SELECT_KEY_INPUT_TOPIC);
        words[8].to(COUNT_INPUT_TOPIC);
    }

    protected String getInputTopic() {
        return BRANCH_INPUT_TOPIC;
    }
}
