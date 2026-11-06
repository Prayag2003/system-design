# Kafka

Apache Kafka is a distributed event streaming platform. It is designed to handle high-throughput, real-time data feeds reliably.

---

## Contents

- [Why Kafka?](#why-kafka)
  - [The Problem: Event-Heavy Applications](#the-problem-event-heavy-applications)
  - [Why Not Just Use APIs?](#why-not-just-use-apis)
  - [Real-World Scale](#real-world-scale)
- [How Kafka Works](#how-kafka-works)
  - [Core Concepts](#core-concepts)
  - [Decoupled Communication](#key-advantage-decoupled-communication)
  - [Offsets](#offsets)
  - [Why "Distributed"?](#why-distributed)

---

## Why Kafka?

### The Problem: Event-Heavy Applications

Consider a food delivery app like Uber Eats or Swiggy. A single order generates a chain of events:

```
User places order -> Order created -> Order placed -> Order accepted
-> Order being prepared -> Order dispatched -> Order delivered -> Order complete
```

Each of these events needs to reach multiple microservices: the restaurant service, delivery partner notifications, customer support, order tracking/telemetry, and more.

### Why Not Just Use APIs?

Direct API calls between services seem like the obvious approach, but they break down at scale:

1. **Combinatorial explosion of requests**
   If 10 events need to reach 10 microservices, that's 100 API calls per order. Scale that to thousands of concurrent orders and you're looking at hundreds of thousands of requests per minute.

2. **Retry overhead**
   When a downstream service rate-limits you (say, 5 req/min), the caller has to handle retries, backoffs, and queuing.

3. **Protocol differences**
   Different microservices might use different API protocols or data formats. The caller needs to know how to talk to each one.

4. **Service discovery**
   In dynamic environments like Kubernetes, IP addresses change constantly. Every caller needs to track DNS or service endpoints for every target.

5. **Security complexity**
   Each service may have its own TLS setup. To secure all inter-service communication, you'd need something like Istio with mTLS (mutual TLS), adding significant operational overhead.

### Real-World Scale

This isn't just a food delivery problem:

- **Stock brokerages** handle millions of market events per minute that need to fan out to analytics, risk engines, user dashboards, and more.
- **Streaming platforms like Netflix** generate thousands of user interaction events per second (plays, pauses, recommendations, buffering metrics).

Direct API-based communication simply doesn't work at this scale. You need a distributed event streaming platform.

---

## How Kafka Works

### Core Concepts

```mermaid
flowchart LR
    P1["Producer 1"] --> T["Topic (in Broker)"]
    P2["Producer 2"] --> T
    T --> C1["Consumer 1"]
    T --> C2["Consumer 2"]
    T --> C3["Consumer 3"]
```

| Term | What it is |
|------|------------|
| **Broker** | A single Kafka server instance (similar to a node in a Kubernetes cluster). It stores and serves data. |
| **Topic** | A named category/feed to which messages are published. Think of it as a channel. Producers write to topics, consumers read from topics. |
| **Producer** | Publishes events/messages to a topic. |
| **Consumer** | Reads events/messages from a topic. Multiple independent consumers can read from the same topic at their own pace. |

### Key Advantage: Decoupled Communication

The producer publishes a message to a topic and moves on. It doesn't care who consumes it or when. This decoupling solves many of the API-based problems above.

**What if a consumer goes down?**

The producer is unaffected. No retries needed on the producer side. Once the consumer comes back up, it picks up right where it left off, thanks to **offsets**.

### Offsets

Every message in a topic has a sequential ID called an offset. A consumer tracks its own offset, so it knows exactly which messages it has already processed. It can even "go back in time" and re-read older messages if needed.

### Why "Distributed"?

A production Kafka setup runs as a **cluster** with multiple brokers, not just one.

```mermaid
flowchart TD
    subgraph Kafka Cluster
        B1["Broker 1"]
        B2["Broker 2"]
        B3["Broker 3"]
    end

    B1 --- B2
    B2 --- B3
    B1 --- B3

    P["Producers"] --> Kafka Cluster
    Kafka Cluster --> C["Consumers"]
```

Topics are spread across all brokers in the cluster through **partitions**. This gives Kafka:

- **Scalability**: Data is distributed across multiple machines.
- **Fault tolerance**: If one broker goes down, other brokers still have copies of the data (via replication).
- **High throughput**: Multiple brokers can serve reads and writes in parallel.

> A topic is a logical concept. Physically, it is split into partitions that live on different brokers.
