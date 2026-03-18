package com.example.java.kafka.producer;

import com.example.java.dto.OrderDto;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

@Service
@Slf4j
public class KafkaProducerService {

    private final String topicName;

    private final KafkaTemplate<Long, OrderDto> kafkaTemplate;

    public KafkaProducerService(@Value("${topic.name}") String topicName, KafkaTemplate<Long, OrderDto> kafkaTemplate) {
        this.topicName = topicName;
        this.kafkaTemplate = kafkaTemplate;
    }

    public CompletableFuture<SendResult<Long, OrderDto>> sendMessage(final OrderDto order) {
        log.info("Sending order: {} to topic: {}", order, topicName);

        final long orderId = order.getId();

        log.info("Attempting to send order: {} with key: {}", order.getId(), orderId);

        return kafkaTemplate.send(topicName, orderId, order).whenComplete((result, exception) -> {
            if (Objects.isNull(exception)) {
                log.info("Successfully sent message to topic {} [partition: {}, offset: {}]",
                        result.getRecordMetadata().topic(),
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());

                log.info("The message {} has been send to the topic {}", order, topicName);

            } else {
                log.error("Unable to send message: {} due to : {}", order, exception.getMessage());
            }
        });
    }
}
