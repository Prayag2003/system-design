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

import java.util.concurrent.atomic.AtomicLong;

/**
 * Consumer Group 5: Real-Time Analytics Service
 * Aggregates order volumes and gross merchandise value (GMV).
 */
@Service
public class AnalyticsConsumer {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsConsumer.class);

    private final AtomicLong totalOrdersCount = new AtomicLong(0);
    private final AtomicLong totalGmvCents = new AtomicLong(0);

    @KafkaListener(
            topics = KafkaTopicConfig.ORDER_EVENTS_TOPIC,
            groupId = "analytics-service"
    )
    public void consume(
            @Payload OrderEvent event,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment
    ) {
        long currentCount = totalOrdersCount.incrementAndGet();
        long addedCents = Math.round(event.getTotalAmount() * 100);
        long currentGmvCents = totalGmvCents.addAndGet(addedCents);

        log.info("[analytics-service] Metric update: Total Orders={}, Current GMV=${}",
                currentCount, String.format("%.2f", currentGmvCents / 100.0));

        acknowledgment.acknowledge();
    }
}
