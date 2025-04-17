package com.example.kafkademo.config;

import com.example.kafkademo.model.OrderEvent;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaRetryConfig {
    @Bean(name = "kafkaRetryListenerContainerFactory")
    public ConcurrentKafkaListenerContainerFactory<String, OrderEvent> kafkaRetryListenerContainerFactory(ConsumerFactory<String, OrderEvent> consumerFactory, KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        ConcurrentKafkaListenerContainerFactory<String, OrderEvent> factory = new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(consumerFactory);

        //DefaultErrorHandler: Spring Kafka 2.8 이상에서 기본 에러 핸들러
        factory.setCommonErrorHandler(
                new DefaultErrorHandler(
                        new DeadLetterPublishingRecoverer(kafkaTemplate), // 실패한 메세지릴 Dead Letter Topic 으로 전송
                        new FixedBackOff(1000L, 3) // 메세지 제처리를 위한 고정 백오프 간격과 재시도 횟수
                )
        );

        return factory;
    }
}
