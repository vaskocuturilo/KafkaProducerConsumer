package com.example.java.service;

import com.example.java.dto.OrderDto;
import org.springframework.kafka.support.SendResult;

import java.util.concurrent.CompletableFuture;

public interface IMessageService {

    CompletableFuture<SendResult<Long, OrderDto>> triggerSend(OrderDto order);

    OrderDto triggerPull();

}
