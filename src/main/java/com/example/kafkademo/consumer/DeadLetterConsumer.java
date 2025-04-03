package com.example.kafkademo.consumer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class DeadLetterConsumer {
    @KafkaListener(topics = "orders.DLT", groupId = "dlt-group")
    public void listenDLT
}
