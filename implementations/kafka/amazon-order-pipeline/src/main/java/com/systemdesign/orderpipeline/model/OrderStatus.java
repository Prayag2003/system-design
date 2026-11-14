package com.systemdesign.orderpipeline.model;

/**
 * Represents the lifecycle stages of an Amazon order.
 */
public enum OrderStatus {
    ORDER_CREATED,
    INVENTORY_RESERVED,
    PAYMENT_COMPLETED,
    WAREHOUSE_NOTIFIED,
    INVOICE_GENERATED,
    ORDER_FAILED
}

