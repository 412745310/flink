package com.chelsea.flink.stream.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Order {
    private String orderId;
    private Integer userId;
    private Integer money;
    private Long createTime;
}
