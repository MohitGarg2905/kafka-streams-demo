package com.kafka.commons;

import com.kafka.oth.PropertiesUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

public class Producer {

    private KafkaProducer<String, byte[]> kafkaProducer ;

    public Producer() {
        this.kafkaProducer = getKafkaProducer();
    }

    public void produceMessageToKafka(String topic, byte[] message,Integer partition,String key) {

        ProducerRecord<String, byte[]> data = new ProducerRecord<>(topic,partition,key,message);
        try {
            System.out.println("Publishing message to "+topic);
            kafkaProducer.send(data);
        } catch (Exception e) {
            System.out.println("Error while producing payload in kafka for topic " + topic);
        }
    }

    private  KafkaProducer<String, byte[]> getKafkaProducer() {
        Properties kafkaProps = PropertiesUtils.getProperties();

        return new KafkaProducer<>(kafkaProps);
    }
}
