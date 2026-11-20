package com.systemdesign.orderpipeline.controller;

import com.systemdesign.orderpipeline.model.AnalyticsSummaryResponse;
import com.systemdesign.orderpipeline.service.RealtimeAnalyticsService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * Controller exposing real-time order processing analytics.
 */
@RestController
@RequestMapping("/api/analytics")
public class AnalyticsController {

    private final RealtimeAnalyticsService analyticsService;

    public AnalyticsController(RealtimeAnalyticsService analyticsService) {
        this.analyticsService = analyticsService;
    }

    @GetMapping("/summary")
    public ResponseEntity<AnalyticsSummaryResponse> getSummary() {
        AnalyticsSummaryResponse summary = new AnalyticsSummaryResponse(
                analyticsService.getTotalOrders(),
                analyticsService.getTotalRevenue(),
                System.currentTimeMillis()
        );
        return ResponseEntity.ok(summary);
    }
}
