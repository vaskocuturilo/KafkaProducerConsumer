package com.example.java.kafka.consumer;

import com.example.java.dto.OrderDto;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.Objects;


@Slf4j
@Service
public class KafkaConsumerService {
    private final DefaultKafkaConsumerFactory<Long, OrderDto> defaultKafkaConsumerFactory;

    public KafkaConsumerService(DefaultKafkaConsumerFactory<Long, OrderDto> defaultKafkaConsumerFactory) {
        this.defaultKafkaConsumerFactory = defaultKafkaConsumerFactory;
    }

    public OrderDto receiveNextMessage(String topic) {
        try (Consumer<Long, OrderDto> consumer = defaultKafkaConsumerFactory.createConsumer()) {
            consumer.subscribe(Collections.singletonList(topic));

            ConsumerRecords<Long, OrderDto> records = consumer.poll(Duration.ofSeconds(5));

            if (Objects.nonNull(records) && !records.isEmpty()) {

                ConsumerRecord<Long, OrderDto> receive = records.iterator().next();

                consumer.commitSync();

                log.info("IN receiveNextMessage:  The message from Kafka has been received => {}", receive.value());

                return receive.value();
            }
        } catch (Exception exception) {
            log.info("IN receiveNextMessage:  The message from Kafka has not been received, return null", exception);
        }

        return null;
    }
}
