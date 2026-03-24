package com.example.java.rest;

import com.example.java.dto.OrderDto;
import com.example.java.service.IMessageService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@RequiredArgsConstructor
@RestController
@RequestMapping("/api/v1/messages")
public class RestControllerV1 {

    private final IMessageService messagesService;
    private static final String MESSAGE = "message";

    @PostMapping("/send")
    public ResponseEntity<Map<String, String>> sendOrderData(@Valid @RequestBody OrderDto order) {
        try {
            messagesService.triggerSend(order).get(5, TimeUnit.SECONDS);

            log.info("The message has been sent to the Kafka {}", order);

            return ResponseEntity.ok(Map.of(MESSAGE, "Message confirmed by Kafka"));
        } catch (final InterruptedException _) {
            Thread.currentThread().interrupt();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(MESSAGE, "Request interrupted"));
        } catch (final ExecutionException | TimeoutException exception) {
            log.error("Kafka delivery failed for order: {}", order.getId(), exception);

            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(MESSAGE, "Kafka failed: " + exception.getMessage()));
        }
    }
}
