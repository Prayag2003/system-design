package com.systemdesign.orderpipeline.service;

import com.systemdesign.orderpipeline.model.OrderStatus;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Service simulating persistent storage for orders (e.g. PostgreSQL/DynamoDB).
 * Tracks the current state of orders across the fulfillment lifecycle.
 */
@Service
public class OrderTrackingService {

    private final Map<String, OrderStatus> orderStatusStore = new ConcurrentHashMap<>();

    public void updateStatus(String orderId, OrderStatus status) {
        orderStatusStore.put(orderId, status);
    }

    public OrderStatus getStatus(String orderId) {
        return orderStatusStore.getOrDefault(orderId, OrderStatus.ORDER_FAILED);
    }

    public boolean exists(String orderId) {
        return orderStatusStore.containsKey(orderId);
    }
}
