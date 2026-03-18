package com.example.java.rest;

import com.example.java.dto.OrderDto;
import com.example.java.service.IMessageService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
@RequiredArgsConstructor
@RestController
@RequestMapping("/api/v1/messages")
public class RestControllerV1 {

    private final IMessageService messagesService;

    @PostMapping("/send")
    public ResponseEntity<Map<String, String>> sendCountryEntityData(@RequestBody OrderDto order) {
        try {
            messagesService.triggerSend(order).get(5, TimeUnit.SECONDS);

            log.info("The message has been send to the Kafka {}", order);

            return ResponseEntity.ok(Map.of("message", "Message confirmed by Kafka"));

        } catch (ExecutionException | InterruptedException | TimeoutException exception) {
            log.error("Kafka delivery failed for order: {}", order.getId(), exception);

            if (exception instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("message", "Kafka failed: " + exception.getMessage()));
        }
    }

    @GetMapping("/pull")
    public ResponseEntity<OrderDto> pullData() {
        OrderDto country = messagesService.triggerPull();

        if (Objects.isNull(country)) {
            log.info("IN pullData: The message has not been pulled from the broker");
            return ResponseEntity.noContent().build();
        }
        log.info("IN pullData: The message has been pulled from the broker");

        return ResponseEntity.ok(country);
    }
}
