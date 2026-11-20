package com.systemdesign.orderpipeline.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Service simulating SMS alert delivery.
 */
@Service
public class SmsNotificationService {

    private static final Logger log = LoggerFactory.getLogger(SmsNotificationService.class);

    public void sendOrderDispatchSms(String phoneNumber, String orderId) {
        log.info("Sending SMS alert to: {}, orderId: {}", phoneNumber, orderId);
    }
}
