package com.kafka.streams;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.Stores;
import org.springframework.stereotype.Component;

import java.time.Duration;

import static com.kafka.commons.Topics.*;

@Component
public class StreamsAggregateAndCount extends StreamsBase{

    protected void createStream(final StreamsBuilder builder) {
        KStream<String, String> source = builder.stream(getInputTopic());
        KStream<String, String> peeked = source.peek((key, value) -> System.out.println("Received on "+getInputTopic()+" Key: "+key+", value: "+value));
        KGroupedStream<String, String> groupedStream = peeked.groupByKey();
        Duration windowDuration = Duration.ofSeconds(10);

        KTable<String, Float> aggregatedStream = groupedStream.aggregate(
                this::initialize, /* initializer */
                this::aggregateAmount, /* adder */
                Materialized.<String, Float>as(Stores.persistentKeyValueStore("aggregated-stream-store"))/* state store name */
                        .withValueSerde(Serdes.Float())); /* serde for aggregate value */

        aggregatedStream.toStream().to(AGGREGATE_OUTPUT_TOPIC);
        groupedStream.count().toStream().to(COUNT_OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));

        KTable<Windowed<String>, Float> aggregatedStreamWindowed = groupedStream.windowedBy(TimeWindows.of(windowDuration)).aggregate(
                this::initialize, /* initializer */
                this::aggregateAmount, /* adder */
                Materialized.<String, Float>as(Stores.persistentWindowStore("aggregated-windowed-stream-store", windowDuration, windowDuration, false))/* state store name */
                        .withValueSerde(Serdes.Float())); /* serde for aggregate value */

        aggregatedStreamWindowed.toStream().map((key, value) -> KeyValue.pair(key.key(),value)).to(AGGREGATE_WINDOWED_OUTPUT_TOPIC, Produced.valueSerde(Serdes.Float()));
        groupedStream.windowedBy(TimeWindows.of(windowDuration)).count().toStream().map((key, value) -> KeyValue.pair(key.key(),value)).to(COUNT_WINDOWED_OUTPUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
    }

    private Float initialize(){
        return 0f;
    }

    private Float aggregateAmount(String key, String newValue, Float aggAmount){
        return aggAmount + Float.parseFloat(newValue.replace("group", ""));
    }

    protected String getInputTopic() {
        return GROUPBY_INPUT_TOPIC;
    }
}
