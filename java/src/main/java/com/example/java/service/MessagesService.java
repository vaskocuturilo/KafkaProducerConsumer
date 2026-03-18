package com.example.java.service;

import com.example.java.dto.OrderDto;
import com.example.java.kafka.consumer.KafkaConsumerService;
import com.example.java.kafka.producer.KafkaProducerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
public class MessagesService implements IMessageService {

    private final KafkaConsumerService kafkaConsumerService;

    private final KafkaProducerService kafkaProducerService;

    private final String topicName;

    public MessagesService(KafkaConsumerService kafkaConsumerService, KafkaProducerService kafkaProducerService, @Value("${topic.name}") String topicName) {
        this.kafkaConsumerService = kafkaConsumerService;

        this.kafkaProducerService = kafkaProducerService;
        this.topicName = topicName;
    }

    @Override
    public CompletableFuture<SendResult<Long, OrderDto>> triggerSend(OrderDto order) {
        log.info("The message {} has been send to the Kafka broker", order);

        return kafkaProducerService.sendMessage(order);
    }

    @Override
    public OrderDto triggerPull() {
        final OrderDto countryDto = kafkaConsumerService.receiveNextMessage(topicName);

        if (Objects.isNull(countryDto)) {
            log.info("IN pullFromBroker:  The message from Kafka has not been received");
            return null;
        }
        log.info("IN pullFromBroker:  The message has been received from the Kafka => {}", countryDto);

        return countryDto;
    }
}
