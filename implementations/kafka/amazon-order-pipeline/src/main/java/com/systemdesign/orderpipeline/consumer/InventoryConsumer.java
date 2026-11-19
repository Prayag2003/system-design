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
 * Consumer Group 1: Inventory Service
 * Responsible for deducting stock when an order is created.
 * Uses manual offset commit (At-Least-Once) and in-memory idempotency checking.
 */
@Service
public class InventoryConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryConsumer.class);

    // In-memory set simulating an idempotency store (e.g. Redis / DB unique constraint)
    private final Set<String> processedEvents = ConcurrentHashMap.newKeySet();

    @KafkaListener(
            topics = KafkaTopicConfig.ORDER_EVENTS_TOPIC,
            groupId = "inventory-service"
    )
    public void consume(
            @Payload OrderEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment
    ) {
        log.info("[inventory-service] Received orderId={} from partition={} offset={}",
                event.getOrderId(), partition, offset);

        // 1. Idempotency Check: prevent duplicate processing on consumer replay
        if (!processedEvents.add(event.getEventId())) {
            log.warn("[inventory-service] Duplicate event detected (eventId={}), skipping stock deduction.",
                    event.getEventId());
            acknowledgment.acknowledge();
            return;
        }

        // 2. Business Logic: Reserve stock for order items
        event.getItems().forEach(item ->
                log.info("[inventory-service] Reserving stock: productId={}, qty={}",
                        item.getProductId(), item.getQuantity())
        );

        // 3. Manual Commit: Acknowledge successful processing to Kafka
        acknowledgment.acknowledge();
        log.info("[inventory-service] Successfully processed and committed offset for orderId={}", event.getOrderId());
    }
}
