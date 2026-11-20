package com.systemdesign.orderpipeline.model;

/**
 * Metrics summary response returned by the analytics API.
 */
public class AnalyticsSummaryResponse {
    private long totalOrders;
    private double totalRevenue;
    private long timestamp;

    public AnalyticsSummaryResponse() {
    }

    public AnalyticsSummaryResponse(long totalOrders, double totalRevenue, long timestamp) {
        this.totalOrders = totalOrders;
        this.totalRevenue = totalRevenue;
        this.timestamp = timestamp;
    }

    public long getTotalOrders() {
        return totalOrders;
    }

    public void setTotalOrders(long totalOrders) {
        this.totalOrders = totalOrders;
    }

    public double getTotalRevenue() {
        return totalRevenue;
    }

    public void setTotalRevenue(double totalRevenue) {
        this.totalRevenue = totalRevenue;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
