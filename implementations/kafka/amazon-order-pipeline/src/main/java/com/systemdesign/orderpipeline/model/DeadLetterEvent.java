package com.systemdesign.orderpipeline.model;

/**
 * Payload published to 'order-events-dlq' when an event fails processing
 * after exhausting retry attempts or encountering an unrecoverable poison pill.
 */
public class DeadLetterEvent {
    private String originalEventId;
    private String orderId;
    private String failedConsumerGroup;
    private String reason;
    private String rawPayload;
    private long failedAt;

    public DeadLetterEvent() {
    }

    public DeadLetterEvent(String originalEventId, String orderId, String failedConsumerGroup,
                           String reason, String rawPayload, long failedAt) {
        this.originalEventId = originalEventId;
        this.orderId = orderId;
        this.failedConsumerGroup = failedConsumerGroup;
        this.reason = reason;
        this.rawPayload = rawPayload;
        this.failedAt = failedAt;
    }

    public String getOriginalEventId() {
        return originalEventId;
    }

    public void setOriginalEventId(String originalEventId) {
        this.originalEventId = originalEventId;
    }

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getFailedConsumerGroup() {
        return failedConsumerGroup;
    }

    public void setFailedConsumerGroup(String failedConsumerGroup) {
        this.failedConsumerGroup = failedConsumerGroup;
    }

    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public String getRawPayload() {
        return rawPayload;
    }

    public void setRawPayload(String rawPayload) {
        this.rawPayload = rawPayload;
    }

    public long getFailedAt() {
        return failedAt;
    }

    public void setFailedAt(long failedAt) {
        this.failedAt = failedAt;
    }

    @Override
    public String toString() {
        return "DeadLetterEvent{" +
                "orderId='" + orderId + '\'' +
                ", failedConsumerGroup='" + failedConsumerGroup + '\'' +
                ", reason='" + reason + '\'' +
                ", failedAt=" + failedAt +
                '}';
    }
}
