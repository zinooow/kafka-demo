package com.example.kafkademo.consumer;

import com.example.kafkademo.model.OrderEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Headers;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Map;

@Service
@Slf4j
public class OrderConsumer {

    @KafkaListener(topics="orders", groupId = "order-group")
    public void listen(
            @Payload OrderEvent order,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        try {
            log.info("Received order:{}, partition: {}, offset: {}", order.getOrderId(), partition, offset);
            processOrder(order);
        } catch (Exception ex) {
            log.error("Error while sending order", ex);
            handleError(order, ex);
        }
    }

    protected void processOrder(OrderEvent order) {
        log.info("Order processed successfully :{}", order.getOrderId());
    }

    protected void handleError(OrderEvent order, Exception e) {
        log.error("Error while sending order", e);
    }

}
