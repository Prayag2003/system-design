package com.systemdesign.orderpipeline.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Service simulating email notification delivery.
 */
@Service
public class EmailNotificationService {

    private static final Logger log = LoggerFactory.getLogger(EmailNotificationService.class);

    public void sendOrderConfirmation(String toEmail, String orderId, double totalAmount) {
        log.info("Sending confirmation email to: {}, orderId: {}, amount: ${}", toEmail, orderId, totalAmount);
    }
}
