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
 * Consumer Group 3: Warehouse Fulfillment Service
 * Generates packing slips and prepares shipments for the fulfillment center.
 */
@Service
public class WarehouseConsumer {

    private static final Logger log = LoggerFactory.getLogger(WarehouseConsumer.class);

    private final Set<String> processedEvents = ConcurrentHashMap.newKeySet();

    @KafkaListener(
            topics = KafkaTopicConfig.ORDER_EVENTS_TOPIC,
            groupId = "warehouse-service"
    )
    public void consume(
            @Payload OrderEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment
    ) {
        log.info("[warehouse-service] Received orderId={} from partition={} offset={}",
                event.getOrderId(), partition, offset);

        if (!processedEvents.add(event.getEventId())) {
            log.warn("[warehouse-service] Duplicate event detected for eventId={}, skipping.",
                    event.getEventId());
            acknowledgment.acknowledge();
            return;
        }

        // Business Logic: Generate packing slip for warehouse staff
        log.info("[warehouse-service] Generating packing slip for orderId={}, itemsCount={}",
                event.getOrderId(), event.getItems() != null ? event.getItems().size() : 0);

        acknowledgment.acknowledge();
        log.info("[warehouse-service] Packing slip created and offset committed for orderId={}", event.getOrderId());
    }
}
