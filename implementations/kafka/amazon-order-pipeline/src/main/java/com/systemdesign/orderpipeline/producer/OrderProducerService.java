package com.systemdesign.orderpipeline.producer;

import com.systemdesign.orderpipeline.config.KafkaTopicConfig;
import com.systemdesign.orderpipeline.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

/**
 * Producer service responsible for publishing order lifecycle events
 * to Kafka using 'orderId' as the partition key.
 */
@Service
public class OrderProducerService {

    private static final Logger log = LoggerFactory.getLogger(OrderProducerService.class);

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    public OrderProducerService(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Publishes an OrderEvent to the 'order-events' topic asynchronously.
     * Uses orderId as key to guarantee partition-level ordering.
     */
    public CompletableFuture<SendResult<String, OrderEvent>> sendOrderEvent(OrderEvent event) {
        String key = event.getOrderId();
        log.info("Publishing OrderEvent [orderId={}, status={}] to topic={}",
                key, event.getStatus(), KafkaTopicConfig.ORDER_EVENTS_TOPIC);

        CompletableFuture<SendResult<String, OrderEvent>> future =
                kafkaTemplate.send(KafkaTopicConfig.ORDER_EVENTS_TOPIC, key, event);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Sent OrderEvent successfully: orderId={}, partition={}, offset={}",
                        key,
                        result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
            } else {
                log.error("Failed to publish OrderEvent: orderId={}, error={}", key, ex.getMessage(), ex);
            }
        });

        return future;
    }
}
