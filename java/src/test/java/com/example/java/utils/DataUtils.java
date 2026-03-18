package com.example.java.utils;

import com.example.java.dto.OrderDto;

import java.math.BigDecimal;

public class DataUtils {

    public static OrderDto getTuvValueDtoPersisted() {
        return OrderDto.builder()
                .id(1L)
                .productId(1L)
                .amount(new BigDecimal(100))
                .build();
    }
}
