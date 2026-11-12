# Amazon Order Pipeline (Spring Boot + Apache Kafka)

A production-grade, event-driven implementation of the **Amazon 7-Step Order Fulfillment Pipeline** built using Java and Spring Boot.

---

## 1. What You Will Learn (Spring Boot & Kafka Fundamentals)

If you are new to Spring Boot, here is how the core concepts map:

| Spring Boot Concept | What It Does | Why We Use It Here |
|---|---|---|
| `@SpringBootApplication` | Entry point of the app | Bootstraps Spring's dependency injection container |
| `@RestController` & `@PostMapping` | Exposes HTTP endpoints | The `/api/orders` checkout endpoint for customers |
| `KafkaTemplate<K, V>` | Spring's Kafka producer client | Sends records to Kafka topics with keys and async callbacks |
| `@KafkaListener` | Spring's Kafka consumer client | Runs a background thread poll-loop for a specific consumer group |
| `@RetryableTopic` | Built-in Dead Letter Queue (DLQ) support | Retries transient failures with backoff, then routes to DLQ |
| `Acknowledgment.acknowledge()` | Manual offset commit | Implements **At-Least-Once** delivery with manual commits |

---

## 2. Architecture & Flow

```
HTTP POST /api/orders
         │
         ▼
[OrderController]
         │
         ▼
[OrderProducerService]
         │
         ▼ (Topic: "order-events", Key: orderId)
    ┌────┴───────────────────────────┬───────────────────────────┐
    ▼                                ▼                           ▼
[InventoryConsumer]          [NotificationConsumer]       [WarehouseConsumer]
Group: inventory-service     Group: notification-service  Group: warehouse-service
```

---

## 3. Project Structure

```
amazon-order-pipeline/
├── pom.xml                                  # Maven dependencies (Spring Boot 3 + Spring Kafka)
├── src/main/resources/
│   └── application.yml                      # Kafka broker, serializers, and consumer configs
└── src/main/java/com/systemdesign/orderpipeline/
    ├── OrderPipelineApplication.java        # Main Spring Boot Runner
    │
    ├── model/                               # Data Contracts (from DESIGN.md)
    │   ├── OrderEvent.java
    │   ├── OrderItem.java
    │   └── OrderStatus.java
    │
    ├── config/                              # Kafka Topic & Error Handling Configurations
    │   └── KafkaTopicConfig.java
    │
    ├── producer/                            # Kafka Producer Service
    │   └── OrderProducerService.java
    │
    ├── consumer/                            # Independent Consumer Groups
    │   ├── InventoryConsumer.java           # Inventory Service
    │   ├── NotificationConsumer.java        # Email & SMS Service
    │   └── WarehouseConsumer.java           # Warehouse Packing Service
    │
    └── controller/                          # REST API for checkout
        └── OrderController.java
```

---

## 4. How to Run & Practice

### Step 1: Start Kafka
Make sure your Kafka cluster is running via Docker:
```bash
cd ../docker
docker compose up -d
```

### Step 2: Run the Spring Boot App
From this directory (`implementations/kafka/amazon-order-pipeline`):
```bash
mvn spring-boot:run
```
Or run `OrderPipelineApplication.java` directly in your IDE (IntelliJ / VS Code / Eclipse).

### Step 3: Trigger an Order (HTTP POST)
```bash
curl -X POST http://localhost:8081/api/orders \
  -H "Content-Type: application/json" \
  -d '{
    "customerId": "cust_101",
    "customerEmail": "alice@example.com",
    "customerPhone": "+1234567890",
    "items": [
      {
        "productId": "prod_apple_iphone",
        "productName": "iPhone 16 Pro",
        "quantity": 1,
        "unitPrice": 999.0
      }
    ],
    "totalAmount": 999.0
  }'
```

### Step 4: Watch the Console & Kafka UI
- Watch the Spring Boot logs print each consumer group (`inventory-service`, `notification-service`, `warehouse-service`) independently processing the event.
- Open **http://localhost:8080** to see consumer lag, offsets, and partition distribution in real time!

