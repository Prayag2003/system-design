package com.systemdesign.orderpipeline.controller;

import com.systemdesign.orderpipeline.model.CreateOrderRequest;
import com.systemdesign.orderpipeline.model.OrderEvent;
import com.systemdesign.orderpipeline.model.OrderResponse;
import com.systemdesign.orderpipeline.model.OrderStatus;
import com.systemdesign.orderpipeline.producer.OrderProducerService;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;

/**
 * REST endpoint exposing checkout operations to customers.
 */
@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderProducerService orderProducerService;

    public OrderController(OrderProducerService orderProducerService) {
        this.orderProducerService = orderProducerService;
    }

    @PostMapping
    public ResponseEntity<OrderResponse> placeOrder(@RequestBody CreateOrderRequest request) {
        String orderId = "ord_" + UUID.randomUUID().toString().substring(0, 8);
        String eventId = UUID.randomUUID().toString();
        long now = System.currentTimeMillis();

        OrderEvent event = new OrderEvent(
                eventId,
                orderId,
                request.getCustomerId(),
                request.getCustomerEmail(),
                request.getCustomerPhone(),
                request.getItems(),
                request.getTotalAmount(),
                OrderStatus.ORDER_CREATED,
                now
        );

        // Publish event asynchronously to Kafka
        orderProducerService.sendOrderEvent(event);

        // Return immediate HTTP 202 Accepted acknowledgment
        OrderResponse response = new OrderResponse(
                orderId,
                OrderStatus.ORDER_CREATED,
                "Order received and queued for processing.",
                now
        );

        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }
}
