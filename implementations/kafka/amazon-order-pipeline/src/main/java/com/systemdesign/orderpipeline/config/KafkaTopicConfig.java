package com.systemdesign.orderpipeline.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Spring Kafka topic configuration.
 * Automatically ensures required topics exist on cluster startup.
 */
@Configuration
public class KafkaTopicConfig {

    public static final String ORDER_EVENTS_TOPIC = "order-events";
    public static final String ORDER_EVENTS_DLQ_TOPIC = "order-events-dlq";

    @Bean
    public NewTopic orderEventsTopic() {
        return TopicBuilder.name(ORDER_EVENTS_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic orderEventsDlqTopic() {
        return TopicBuilder.name(ORDER_EVENTS_DLQ_TOPIC)
                .partitions(1)
                .replicas(1)
                .build();
    }
}
