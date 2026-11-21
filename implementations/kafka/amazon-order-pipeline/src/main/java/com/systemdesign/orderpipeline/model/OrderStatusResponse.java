package com.systemdesign.orderpipeline.model;

/**
 * Detailed order status response returned by GET /api/orders/{orderId}.
 */
public class OrderStatusResponse {
    private String orderId;
    private OrderStatus status;
    private long lastUpdated;

    public OrderStatusResponse() {
    }

    public OrderStatusResponse(String orderId, OrderStatus status, long lastUpdated) {
        this.orderId = orderId;
        this.status = status;
        this.lastUpdated = lastUpdated;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public OrderStatus getStatus() {
        return status;
    }

    public void setStatus(OrderStatus status) {
        this.status = status;
    }

    public long getLastUpdated() {
        return lastUpdated;
    }

    public void setLastUpdated(long lastUpdated) {
        this.lastUpdated = lastUpdated;
    }
}
