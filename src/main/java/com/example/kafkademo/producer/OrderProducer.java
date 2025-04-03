package com.example.kafkademo.producer;

import com.example.kafkademo.model.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class OrderProducer {
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;
    private static final String TOPIC = "orders";

    public void sendOrder(OrderEvent order) {
        kafkaTemplate.send(TOPIC, order.getOrderId(), order)
                .whenComplete((result, ex) -> {
                    if(ex != null) {
                        log.error("Error while sending order", ex);
                    } else {
                        log.info("Order sent successfully :{}, partition: {}", order.getOrderId(), result.getRecordMetadata().partition());
                    }
                });
    }
    public void sendOrderSync(OrderEvent order) {
        try {
            SendResult<String, OrderEvent> result = kafkaTemplate.send(TOPIC, order.getOrderId(), order).get();
            log.info("Order sent successfully :{}, partition: {}", order.getOrderId(), result.getRecordMetadata().partition());
        } catch (Exception ex) {
            log.error("Error while sending order", ex);
        }
    }
}
