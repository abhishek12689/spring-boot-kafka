package com.dwivedi.kafka.service;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
public class ConsumerService {

    @KafkaListener(topics = "${spring.kafka.topic.name}", containerFactory = "kafkaListenerContainerFactory")
    public void consumeMessage(@Payload String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topicName, @Header(KafkaHeaders.OFFSET) String offset) {
        System.out.println("Received message=[" + message + "] with offset=[" + offset + "] from topic=[" + topicName + "]");
    }
}
