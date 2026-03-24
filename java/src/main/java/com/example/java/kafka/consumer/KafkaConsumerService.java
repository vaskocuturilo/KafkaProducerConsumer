package com.example.java.kafka.consumer;

import com.example.java.dto.OrderDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;


@Service
@Slf4j
public class KafkaConsumerService {

    @KafkaListener(
            topics = "${topic.name}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    @KafkaHandler
    public void consumeMessage(
            @Payload OrderDto order,
            @Header(KafkaHeaders.RECEIVED_TOPIC) String topic,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset
    ) {
        log.info("Successfully consumed from Topic: {}, Partition: {}, Offset: {}", topic, partition, offset);
        log.info("Processing Order Data: ID: = {} (USER_ID: {}), ORDER: {}", order.getId(), order.getUserId(), order);
    }

    @KafkaHandler(isDefault = true)
    public void handleUnknown(Object unknown) {
        log.warn("Received unknown message type: {}", unknown.getClass().getSimpleName());
    }
}
