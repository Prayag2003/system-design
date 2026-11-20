package com.systemdesign.orderpipeline.consumer;

import com.systemdesign.orderpipeline.config.KafkaTopicConfig;
import com.systemdesign.orderpipeline.model.DeadLetterEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

/**
 * Consumer dedicated to monitoring and processing events from the Dead Letter Queue ('order-events-dlq').
 * Used for alerting on-call engineers, offline auditing, or triggering manual reconciliation.
 */
@Service
public class DeadLetterQueueConsumer {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterQueueConsumer.class);

    @KafkaListener(
            topics = KafkaTopicConfig.ORDER_EVENTS_DLQ_TOPIC,
            groupId = "dlq-monitoring-service"
    )
    public void consume(
            @Payload DeadLetterEvent dlqEvent,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment
    ) {
        log.error("[ALERT] Poison pill received in DLQ: orderId={}, failedService={}, reason={}, originalEventId={}",
                dlqEvent.getOrderId(),
                dlqEvent.getFailedConsumerGroup(),
                dlqEvent.getReason(),
                dlqEvent.getOriginalEventId());

        // Here you would trigger PagerDuty/Slack alerts or persist to an audit log store

        acknowledgment.acknowledge();
    }
}
