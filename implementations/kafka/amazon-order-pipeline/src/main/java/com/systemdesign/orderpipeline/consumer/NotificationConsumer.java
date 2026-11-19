package com.systemdesign.orderpipeline.consumer;

import com.systemdesign.orderpipeline.config.KafkaTopicConfig;
import com.systemdesign.orderpipeline.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Consumer Group 2: Notification Service
 * Responsible for dispatching order confirmation emails and SMS alerts to the customer.
 * Demonstrates independent fan-out: runs in its own consumer group ("notification-service")
 * with independent offsets from inventory-service.
 */
@Service
public class NotificationConsumer {

    private static final Logger log = LoggerFactory.getLogger(NotificationConsumer.class);

    private final Set<String> sentNotifications = ConcurrentHashMap.newKeySet();

    @KafkaListener(
            topics = KafkaTopicConfig.ORDER_EVENTS_TOPIC,
            groupId = "notification-service"
    )
    public void consume(
            @Payload OrderEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment
    ) {
        log.info("[notification-service] Processing notifications for orderId={} from partition={} offset={}",
                event.getOrderId(), partition, offset);

        // 1. Idempotency Check: prevent sending duplicate emails/SMS
        if (!sentNotifications.add(event.getEventId())) {
            log.warn("[notification-service] Notification already sent for eventId={}, skipping.",
                    event.getEventId());
            acknowledgment.acknowledge();
            return;
        }

        // 2. Business Logic: Dispatch Email and SMS
        sendEmail(event.getCustomerEmail(), event.getOrderId(), event.getTotalAmount());
        sendSms(event.getCustomerPhone(), event.getOrderId());

        // 3. Commit Offset: acknowledge successful delivery
        acknowledgment.acknowledge();
        log.info("[notification-service] Notifications completed and offset committed for orderId={}", event.getOrderId());
    }

    private void sendEmail(String email, String orderId, double amount) {
        log.info("[Email Service] Sent confirmation email to {} for orderId={} (total: ${})", email, orderId, amount);
    }

    private void sendSms(String phone, String orderId) {
        log.info("[SMS Gateway] Sent dispatch SMS to {} for orderId={}", phone, orderId);
    }
}
