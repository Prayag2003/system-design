package com.systemdesign.orderpipeline.service;

import com.systemdesign.orderpipeline.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Service managing warehouse dispatch orders and packing slip generation.
 */
@Service
public class WarehouseFulfillmentService {

    private static final Logger log = LoggerFactory.getLogger(WarehouseFulfillmentService.class);

    public String generatePackingSlip(OrderEvent event) {
        String slipId = "SLIP-" + event.getOrderId();
        log.info("Generated packing slip: {} for customer: {}", slipId, event.getCustomerId());
        return slipId;
    }
}
