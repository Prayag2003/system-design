package com.systemdesign.orderpipeline.service;

import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicLong;

/**
 * In-memory service maintaining real-time business performance metrics.
 */
@Service
public class RealtimeAnalyticsService {

    private final AtomicLong totalOrders = new AtomicLong(0);
    private final AtomicLong totalRevenueCents = new AtomicLong(0);

    public void recordOrder(double amount) {
        totalOrders.incrementAndGet();
        totalRevenueCents.addAndGet(Math.round(amount * 100));
    }

    public long getTotalOrders() {
        return totalOrders.get();
    }

    public double getTotalRevenue() {
        return totalRevenueCents.get() / 100.0;
    }
}
