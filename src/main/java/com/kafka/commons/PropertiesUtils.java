package com.kafka.commons;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.springframework.context.annotation.Configuration;

import java.util.Collections;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

@Configuration
public class PropertiesUtils {

    public static Properties getProperties(String applicationId){
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return props;
    }

    public static Properties getProperties(){
        return getProperties("kafka-streams-apis");
    }

    public static void createTopic(String topic, Integer noOfPartition, Short replicationFactor) throws ExecutionException, InterruptedException {
        try (AdminClient adminClient = AdminClient.create(getProperties(topic))) {
            Set<String> topicsList = adminClient.listTopics().names().get();
            if(!topicsList.contains(topic)){
                System.out.println("Creating topic " + topic);
                adminClient.createTopics(Collections.singletonList(new NewTopic(topic, noOfPartition, replicationFactor)));
            }else{
                System.out.println("Topic "+topic+" exists");
            }
        }
    }
}
