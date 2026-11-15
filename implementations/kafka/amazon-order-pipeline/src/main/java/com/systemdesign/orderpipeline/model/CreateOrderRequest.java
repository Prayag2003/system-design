package com.systemdesign.orderpipeline.model;

import java.util.List;

/**
 * Incoming request payload submitted by the customer / client
 * via POST /api/orders.
 */
public class CreateOrderRequest {
    private String customerId;
    private String customerEmail;
    private String customerPhone;
    private List<OrderItem> items;
    private double totalAmount;

    public CreateOrderRequest() {
    }

    public CreateOrderRequest(String customerId, String customerEmail, String customerPhone,
                              List<OrderItem> items, double totalAmount) {
        this.customerId = customerId;
        this.customerEmail = customerEmail;
        this.customerPhone = customerPhone;
        this.items = items;
        this.totalAmount = totalAmount;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getCustomerEmail() {
        return customerEmail;
    }

    public void setCustomerEmail(String customerEmail) {
        this.customerEmail = customerEmail;
    }

    public String getCustomerPhone() {
        return customerPhone;
    }

    public void setCustomerPhone(String customerPhone) {
        this.customerPhone = customerPhone;
    }

    public List<OrderItem> getItems() {
        return items;
    }

    public void setItems(List<OrderItem> items) {
        this.items = items;
    }

    public double getTotalAmount() {
        return totalAmount;
    }

    public void setTotalAmount(double totalAmount) {
        this.totalAmount = totalAmount;
    }

    @Override
    public String toString() {
        return "CreateOrderRequest{" +
                "customerId='" + customerId + '\'' +
                ", customerEmail='" + customerEmail + '\'' +
                ", itemsCount=" + (items != null ? items.size() : 0) +
                ", totalAmount=" + totalAmount +
                '}';
    }
}
