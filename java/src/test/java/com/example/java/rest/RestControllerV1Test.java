package com.example.java.rest;

import com.example.java.dto.OrderDto;
import com.example.java.service.IMessageService;
import com.example.java.utils.DataUtils;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.BDDMockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.kafka.support.SendResult;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@ExtendWith(SpringExtension.class)
@WebMvcTest(RestControllerV1.class)
class RestControllerV1Test {

    @MockitoBean
    private IMessageService messagesService;

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private ObjectMapper objectMapper;

    private static final String ENDPOINT_PATH = "/api/v1/messages";

    @Test
    @DisplayName("Test send order to Kafka - success")
    void givenOrder_whenSend_thenSuccessResponse() throws Exception {
        final OrderDto order = DataUtils.buildSampleOrder();

        final CompletableFuture<SendResult<Long, OrderDto>> future = CompletableFuture.completedFuture(mock(SendResult.class));

        BDDMockito.given(messagesService.triggerSend(any())).willReturn(future);

        mockMvc.perform(post(ENDPOINT_PATH + "/send")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(order)))
                .andDo(MockMvcResultHandlers.print())
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.message", CoreMatchers.is("Message confirmed by Kafka")));
    }

    @Test
    @DisplayName("Test send order to Kafka - timeout failure")
    void givenOrder_whenSendTimesOut_thenErrorResponse() throws Exception {
        final OrderDto order = DataUtils.buildSampleOrder();

        final CompletableFuture<SendResult<Long, OrderDto>> future = new CompletableFuture<>();

        future.completeExceptionally(new ExecutionException("Kafka timeout", new TimeoutException()));

        BDDMockito.given(messagesService.triggerSend(any())).willReturn(future);

        mockMvc.perform(post(ENDPOINT_PATH + "/send")
                        .contentType(MediaType.APPLICATION_JSON)
                        .content(objectMapper.writeValueAsString(order)))
                .andDo(MockMvcResultHandlers.print())
                .andExpect(status().isInternalServerError())
                .andExpect(jsonPath("$.message", CoreMatchers.containsString("Kafka failed")));
    }
}