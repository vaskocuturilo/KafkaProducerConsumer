package com.example.java.kafka.consumer;

import com.example.java.dto.OrderDto;
import com.example.java.utils.DataUtils;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;
import static org.awaitility.Awaitility.await;

@SpringBootTest
@EmbeddedKafka(
        partitions = 1,
        brokerProperties = {
                "listeners=PLAINTEXT://localhost:9092",
                "port=9092"
        })
@ActiveProfiles("test")
class KafkaConsumerServiceTest {
    @Autowired
    private KafkaTemplate<Long, OrderDto> testKafkaTemplate;

    @MockitoSpyBean
    private KafkaConsumerService kafkaConsumerService;

    @Value("${topic.name}")
    private String topic;

    @Test
    void shouldConsumeMessageSuccessfully() {
        // given
        final OrderDto payload = DataUtils.getTuvValueDtoPersisted();

        // when
        testKafkaTemplate.send(topic, 1L, payload);

        // then
        await().atMost(5, TimeUnit.SECONDS)
                .untilAsserted(() -> {
                    OrderDto result = kafkaConsumerService.receiveNextMessage(topic);

                    assertThat(result).isNotNull();
                    assertThat(result.getId()).isEqualTo(payload.getId());
                    assertThat(result.getProductId()).isEqualTo(payload.getProductId());
                    assertThat(result.getAmount()).isEqualTo(payload.getAmount());
                });
    }

    @Test
    void shouldReturnNullWhenNoMessageAvailable() {
        final OrderDto result = kafkaConsumerService.receiveNextMessage(topic);

        // then
        assertThat(result).isNull();
    }
}