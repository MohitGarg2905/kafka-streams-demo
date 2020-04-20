package com.kafka.controllers;

import com.kafka.commons.Producer;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.*;

import javax.annotation.PostConstruct;

import static com.kafka.commons.Topics.BRANCH_INPUT_TOPIC;

@RestController
@RequestMapping("/kafka/streams/demo")
public class KafkaStreamsDemoController {

    @PostConstruct
    public void setup(){
        System.out.println("Starting");
    }

    @GetMapping(value = "/branch")
    public void demoBranch(@RequestParam(name = "value") String value){
        Producer producer = new Producer();
        producer.produceMessageToKafka(BRANCH_INPUT_TOPIC, value.getBytes(), 1, value);
    }
}
