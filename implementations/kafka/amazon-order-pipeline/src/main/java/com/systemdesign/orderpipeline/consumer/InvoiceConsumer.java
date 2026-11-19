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
 * Consumer Group 4: Invoice Generation Service
 * Renders invoices and records billing information.
 */
@Service
public class InvoiceConsumer {

    private static final Logger log = LoggerFactory.getLogger(InvoiceConsumer.class);

    private final Set<String> processedEvents = ConcurrentHashMap.newKeySet();

    @KafkaListener(
            topics = KafkaTopicConfig.ORDER_EVENTS_TOPIC,
            groupId = "invoice-service"
    )
    public void consume(
            @Payload OrderEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment
    ) {
        log.info("[invoice-service] Processing billing for orderId={} from partition={} offset={}",
                event.getOrderId(), partition, offset);

        if (!processedEvents.add(event.getEventId())) {
            log.warn("[invoice-service] Duplicate event detected for eventId={}, skipping invoice generation.",
                    event.getEventId());
            acknowledgment.acknowledge();
            return;
        }

        // Business Logic: Generate tax invoice
        log.info("[invoice-service] Generated invoice INV-{} for customer={} (Amount: ${})",
                event.getOrderId(), event.getCustomerId(), event.getTotalAmount());

        acknowledgment.acknowledge();
        log.info("[invoice-service] Invoice rendered and offset committed for orderId={}", event.getOrderId());
    }
}
