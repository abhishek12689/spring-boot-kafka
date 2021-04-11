package com.dwivedi.kafka.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

@Service
public class ProducerService {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    @Value("${spring.kafka.topic.name}")
    private String topicName;

    public boolean sendMessage(String message) {
        boolean isSuccessful = false;
        ListenableFuture future = kafkaTemplate.send(topicName, "Key-1", message);
        try {
            SendResult<String, String> result = (SendResult<String, String>) future.get();
            isSuccessful = future.isDone();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }

        future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {
            @Override
            public void onSuccess(SendResult<String, String> result) {
                System.out.println("Published message=[" + message + "] " +
                        "with offset=[" + result.getRecordMetadata().offset() + "] " +
                        "to topic=[" + result.getRecordMetadata().topic() + "]");
            }

            @Override
            public void onFailure(Throwable throwable) {
                System.out.println("Unable to send message=[" + message + "], Reason=[" + throwable.getMessage() + "]");
            }
        });
        return isSuccessful;
    }
}
