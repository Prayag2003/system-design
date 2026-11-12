# Kafka Implementations

Hands-on projects and environments for Apache Kafka.

---

## Directory Structure

```
implementations/kafka/
├── DESIGN.md                  # Complete High-Level (HLD) & Low-Level Design (LLD)
│
├── docker/                    # 1. Local environment setup
│   ├── docker-compose.yml     # KRaft Kafka Broker + Kafka UI dashboard
│   └── README.md              # CLI practice, segment logs inspection, and screenshots
│
└── amazon-order-pipeline/     # 2. Full Spring Boot application
    ├── pom.xml                # Spring Boot 3 + Spring Kafka
    ├── src/                   # Source code (Producer, Consumers, DLQ, REST API)
    └── README.md              # Quickstart guide & learning roadmap
```

## Modules

| Module | What It Covers | Link |
|---|---|---|
| **System Architecture** | HLD and LLD for the Amazon 7-step order fulfillment pipeline | [DESIGN.md](DESIGN.md) |
| **Docker Lab** | Local KRaft setup, CLI commands, physical file structure inspection, and UI | [docker/README.md](docker/README.md) |
| **Spring Boot Pipeline** | Real-world Java/Spring Boot event-driven pipeline | [amazon-order-pipeline/README.md](amazon-order-pipeline/README.md) |

