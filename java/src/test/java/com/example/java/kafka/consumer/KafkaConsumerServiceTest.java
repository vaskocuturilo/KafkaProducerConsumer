package com.example.java.kafka.consumer;

import com.example.java.dto.OrderDto;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@SpringBootTest
@EmbeddedKafka(partitions = 1)
@ActiveProfiles("test")
class KafkaConsumerServiceTest {

    @Autowired
    private KafkaTemplate<Long, OrderDto> kafkaTemplate;

    @SpyBean
    private KafkaConsumerService kafkaConsumerService;

    @Value("${topic.name}")
    private String topic;

    @Test
    void shouldConsumeMessageSuccessfully() {
        OrderDto payload = new OrderDto(1L, 1L, 1L, new BigDecimal(100));

        kafkaTemplate.send(topic, 1L, payload);

        await().atMost(5, TimeUnit.SECONDS).untilAsserted(() -> {
            verify(kafkaConsumerService, times(1))
                    .consumeMessage(eq(payload), anyString(), anyInt(), anyLong());
        });
    }
}