package com.example.java.kafka.producer;

import com.example.java.dto.OrderDto;
import com.example.java.utils.DataUtils;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class KafkaProducerServiceTest {

    @Mock
    private KafkaTemplate<Long, OrderDto> kafkaTemplate;

    private KafkaProducerService kafkaProducerService;

    private final String topicName = "test-topic";

    @BeforeEach
    void setUp() {
        kafkaProducerService = new KafkaProducerService(topicName, kafkaTemplate);
    }

    @Test
    void givenOrder_SendMessage_Success() {
        //given
        OrderDto orderDto = DataUtils.buildSampleOrder();

        //when
        final CompletableFuture<SendResult<Long, OrderDto>> future = new CompletableFuture<>();

        when(kafkaTemplate.send(anyString(), anyLong(), any(OrderDto.class))).thenReturn(future);

        kafkaProducerService.sendMessage(orderDto);

        //then
        verify(kafkaTemplate, times(1)).send(topicName, 1L, orderDto);
    }

    @Test
    void givenOrder_SendMessage_LogsSuccessOnCompletion() {
        final OrderDto orderDto = DataUtils.buildSampleOrder();

        final SendResult<Long, OrderDto> sendResult = mock(SendResult.class);

        final RecordMetadata meta = new RecordMetadata(new TopicPartition(topicName, 0), 0, 0, 0, 0, 0);

        when(sendResult.getRecordMetadata()).thenReturn(meta);

        CompletableFuture<SendResult<Long, OrderDto>> future = CompletableFuture.completedFuture(sendResult);

        when(kafkaTemplate.send(anyString(), anyLong(), any())).thenReturn(future);

        kafkaProducerService.sendMessage(orderDto).join();

        verify(kafkaTemplate).send(topicName, 1L, orderDto);
    }
}