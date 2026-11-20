package com.systemdesign.orderpipeline.service;

import com.systemdesign.orderpipeline.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Service responsible for billing document and invoice PDF generation.
 */
@Service
public class BillingInvoiceService {

    private static final Logger log = LoggerFactory.getLogger(BillingInvoiceService.class);

    public String createInvoice(OrderEvent event) {
        String invoiceNumber = "INV-" + event.getOrderId().toUpperCase();
        log.info("Rendered tax invoice: {} for amount: ${}", invoiceNumber, event.getTotalAmount());
        return invoiceNumber;
    }
}
