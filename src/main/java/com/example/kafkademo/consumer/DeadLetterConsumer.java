package com.example.kafkademo.consumer;

import com.example.kafkademo.model.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class DeadLetterConsumer {
    @KafkaListener(topics = "order.DLT", groupId = "dlt_group")
    public void listenDLT(@Payload OrderEvent order, Exception ex) {
        log.error("Received fail order in DLT: {}, Error: {}", order.getOrderId(), ex.getMessage());
    }
}
