package com.systemdesign.orderpipeline.producer;

import com.systemdesign.orderpipeline.config.KafkaTopicConfig;
import com.systemdesign.orderpipeline.model.DeadLetterEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * Service to publish unprocessable poison pills and exhausted-retry events
 * to the dead letter queue ('order-events-dlq').
 */
@Service
public class DeadLetterQueueProducer {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterQueueProducer.class);

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public DeadLetterQueueProducer(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void sendToDlq(DeadLetterEvent dlqEvent) {
        log.warn("Routing poison pill to DLQ topic={} for orderId={}, consumerGroup={}, reason={}",
                KafkaTopicConfig.ORDER_EVENTS_DLQ_TOPIC,
                dlqEvent.getOrderId(),
                dlqEvent.getFailedConsumerGroup(),
                dlqEvent.getReason());

        kafkaTemplate.send(KafkaTopicConfig.ORDER_EVENTS_DLQ_TOPIC, dlqEvent.getOrderId(), dlqEvent)
                .whenComplete((result, ex) -> {
                    if (ex == null) {
                        log.info("Successfully recorded DLQ event at partition={}, offset={}",
                                result.getRecordMetadata().partition(),
                                result.getRecordMetadata().offset());
                    } else {
                        log.error("Failed to forward event to DLQ: {}", ex.getMessage(), ex);
                    }
                });
    }
}
