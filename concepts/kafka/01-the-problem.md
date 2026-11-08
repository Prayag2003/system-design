# The Problem: Why We Need Kafka

---

## Contents

- [Event-Heavy Applications](#event-heavy-applications)
- [Why Not Just Use APIs?](#why-not-just-use-apis)
- [The Synchronous Trap](#the-synchronous-trap)
- [Real-World Scale](#real-world-scale)
- [Async Processing Options](#async-processing-options)
  - [Background Threads](#1-background-threads)
  - [Database Polling (Outbox)](#2-database-polling-outbox)
  - [Cron Jobs](#3-cron-jobs)
  - [Webhooks](#4-webhooks)
  - [Serverless / Task Services](#5-serverless--task-services)
  - [Message Queues](#6-message-queues)
  - [Event Streaming (Kafka)](#7-event-streaming-kafka)
  - [Comparison](#comparison)
  - [Message Queue vs Event Streaming](#message-queue-vs-event-streaming)

---

## Event-Heavy Applications

Consider a food delivery app like Uber Eats or Swiggy. A single order generates a chain of events:

```
User places order -> Order created -> Order placed -> Order accepted
-> Order being prepared -> Order dispatched -> Order delivered -> Order complete
```

Each of these events needs to reach multiple microservices: the restaurant service, delivery partner notifications, customer support, order tracking/telemetry, and more.

Now consider an Amazon order. When a user places an order, all of this needs to happen:

1. Save order to DB
2. Reduce inventory
3. Send confirmation email
4. Send SMS
5. Notify warehouse
6. Generate invoice
7. Update analytics

That's 7 downstream actions from a single user click.

## Why Not Just Use APIs?

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

## The Synchronous Trap

What if we chain all those services synchronously?

```
User -> Order Service -> Inventory -> Email -> SMS -> Analytics
```

This fails for several reasons:

1. **User waits for everything to finish.** The response time is the sum of all downstream calls.
2. **Slowest service bottlenecks everything.** If the email service is slow, the entire order API is slow.
3. **Any failure breaks the chain.** If SMS is down, the order fails, even though SMS has nothing to do with placing an order.
4. **Tight coupling.** The user shouldn't care about which backend services exist. These are internal concerns.

**Think of it like standing in a queue to collect food.** If you can't leave until the chef finishes cooking for everyone ahead of you, you're blocked even though the delay has nothing to do with your order. The better approach: place your order, get a token, go do whatever you want, and come back when it's ready. That's async processing.

## Real-World Scale

This isn't just an e-commerce problem:

- **Stock brokerages** handle millions of market events per minute that need to fan out to analytics, risk engines, user dashboards, and more.
- **Streaming platforms like Netflix** generate thousands of user interaction events per second (plays, pauses, recommendations, buffering metrics).
- **YouTube video uploads** trigger transcoding to multiple resolutions (360p, 720p, 1080p, 4K), copyright checks, analytics, and thumbnail generation. The user shouldn't be blocked waiting for all of that.

Direct synchronous communication simply doesn't work at this scale. You need async processing.

---

## Async Processing Options

There are several ways to handle async work. Each has trade-offs, and Kafka is not always the right answer.

### 1. Background Threads

Run tasks in application threads alongside your main API thread.

```
API thread -> submit task -> Background thread
```

| Pros | Cons |
|------|------|
| Very simple | If the service crashes, jobs are lost |
| No extra infra needed | No retries |
| Low latency | No durability |
| Good for tiny apps | Not scalable across machines |

### 2. Database Polling (Outbox)

Store tasks in a DB table. A separate worker polls the table and processes tasks.

```
Order Service -> DB (task table) -> Worker polls DB -> Processes task
```

| Pros | Cons |
|------|------|
| Durable (tasks survive crashes) | Polling is inefficient |
| Easy to implement | DB load increases with scale |
| Simple retry logic | Scaling workers is painful |
| | Maintaining order is hard |

### 3. Cron Jobs

Tasks run at fixed intervals (e.g., daily report generation, batch processing, maintenance).

| Pros | Cons |
|------|------|
| Simple | Not real-time |
| Predictable execution | Inherent delay |
| No event dependency | Poor for user-triggered actions |

### 4. Webhooks

Your system calls someone else's HTTP endpoint asynchronously. Common for payment integrations (Razorpay, Stripe) and 3rd-party notifications.

| Pros | Cons |
|------|------|
| Simple, no extra infra | Network failures |
| Good for 3rd-party integrations | Retry complexity |
| | Tight coupling with external service |

### 5. Serverless / Task Services

Managed async execution using services like AWS Lambda, Google Cloud Tasks, or Azure Functions.

| Pros | Cons |
|------|------|
| Auto-scaling based on load | Cold starts |
| No server management | Vendor lock-in |
| Pay per use | Execution limits (e.g., Lambda concurrency) |

### 6. Message Queues

A queue sits between the producer and consumer. Messages are stored temporarily and removed once consumed.

```
Producer -> Message Queue -> Consumer
```

Examples: RabbitMQ, Amazon SQS, ActiveMQ.

Key trait: once a message is consumed, it's gone from the queue.

### 7. Event Streaming (Kafka)

Different from a classic message queue. Events are published to topics and persist. Multiple consumer groups can read the same events independently.

```
Producer -> Topic -> Consumer Group A
                  -> Consumer Group B
                  -> Consumer Group C
```

Key trait: events are retained (not deleted on consumption), so consumers can replay, catch up, or process at their own pace.

### Comparison

| Option | Reliable | Scalable | Real-time | Best for |
|--------|----------|----------|-----------|----------|
| Background threads | No | No | Yes | Tiny apps, fire-and-forget |
| DB polling (Outbox) | Yes | Limited | Delayed | Simple workflows, low throughput |
| Cron jobs | Yes | Limited | No | Batch processing, reports |
| Webhooks | Partial | Partial | Yes | 3rd-party integrations |
| Serverless | Yes | Yes | Yes | Stateless, bursty workloads |
| Message queues | Yes | Yes | Yes | Task distribution, work queues |
| **Event streaming (Kafka)** | **Yes** | **Yes** | **Yes** | **High-throughput, multi-consumer event processing** |

### Message Queue vs Event Streaming

These two are often confused, but they solve different problems.

**Message Queue** (think: task queue / to-do list)
- Each message is a job to be done.
- Process it once, then it's gone from the queue.
- Typically one consumer per message.
- Example: send a confirmation email for an order and forget it.
- Examples: RabbitMQ, Amazon SQS.

**Event Streaming** (think: immutable log / history)
- Each event is a fact that happened. It never changes.
- Multiple consumers can read the same event independently.
- Events can be replayed at any time.
- Retention is configurable (e.g., 7 days, 30 days, or indefinite).
- Consumption does not delete the event.
- Example: Apache Kafka.

| Feature | Message Queue | Event Streaming |
|---------|---------------|-----------------|
| Who consumes? | One consumer per message | Many consumers independently |
| Message removal | After consumption | Never (until retention expires) |
| Replay messages | No | Yes |
| Consumer state | Queue manages position | Consumer manages its own offset |

> Kafka can act as both a message queue and an event streaming platform, but it is primarily used as an event streaming system.