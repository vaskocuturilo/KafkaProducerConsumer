package com.example.java.dto;

import jakarta.validation.constraints.DecimalMin;
import lombok.*;

import java.math.BigDecimal;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class OrderDto {
    @NonNull
    private Long id;
    @NonNull
    private Long productId;
    @NonNull
    private Long userId;
    @NonNull
    @DecimalMin("0.00")
    private BigDecimal amount;
}
