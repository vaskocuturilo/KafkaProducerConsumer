package com.example.java.service;

import com.example.java.dto.OrderDto;
import com.example.java.kafka.producer.KafkaProducerService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Slf4j
@Service
public class MessagesService implements IMessageService {

    private final KafkaProducerService kafkaProducerService;

    public MessagesService(KafkaProducerService kafkaProducerService) {
        this.kafkaProducerService = kafkaProducerService;
    }

    @Override
    public CompletableFuture<SendResult<Long, OrderDto>> triggerSend(OrderDto order) {
        log.info("The message {} has been send to the Kafka broker", order);

        return kafkaProducerService.sendMessage(order);
    }
}
