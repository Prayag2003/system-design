# Kafka Interview Q&A

A curated collection of practical, conceptual, and architectural questions frequently asked in system design and backend engineering interviews.

---

## Contents

- [Core Architecture & Concepts](#core-architecture--concepts)
  - [Q1. Can a partition contain events from multiple topics?](#q1-can-a-partition-contain-events-from-multiple-topics)
  - [Q2. Do we create new partitions for every new topic?](#q2-do-we-create-new-partitions-for-every-new-topic)
  - [Q3. How does physical data look on disk?](#q3-how-does-physical-data-look-on-disk)
  - [Q4. What is the difference between a Topic and an Event?](#q4-what-is-the-difference-between-a-topic-and-an-event)
  - [Q5. What exactly is stored inside `__consumer_offsets`?](#q5-what-exactly-is-stored-inside-__consumer_offsets)
  - [Q6. Why is Kafka so fast despite writing everything to disk?](#q6-why-is-kafka-so-fast-despite-writing-everything-to-disk)
- [Consumers & Partition Assignment](#consumers--partition-assignment)
  - [Q7. Can a consumer in a consumer group consume from multiple topics?](#q7-can-a-consumer-in-a-consumer-group-consume-from-multiple-topics)
  - [Q8. Can a consumer in a consumer group consume from multiple partitions of the same topic?](#q8-can-a-consumer-in-a-consumer-group-consume-from-multiple-partitions-of-the-same-topic)
  - [Q9. Can two consumers in the same consumer group read from the same partition?](#q9-can-two-consumers-in-the-same-consumer-group-read-from-the-same-partition)
  - [Q10. What happens if consumers in a consumer group exceed the number of partitions?](#q10-what-happens-if-consumers-in-a-consumer-group-exceed-the-number-of-partitions)
  - [Q11. What triggers a Consumer Group Rebalance, and what are its drawbacks?](#q11-what-triggers-a-consumer-group-rebalance-and-what-are-its-drawbacks)
- [Fault Tolerance & Guarantees](#fault-tolerance--guarantees)
  - [Q12. What happens when a consumer crashes before committing its offset?](#q12-what-happens-when-a-consumer-crashes-before-committing-its-offset)
  - [Q13. Can offsets ever go backwards?](#q13-can-offsets-ever-go-backwards)
  - [Q14. How does Kafka prevent data loss if a leader broker crashes?](#q14-how-does-kafka-prevent-data-loss-if-a-leader-broker-crashes)
  - [Q15. What are the trade-offs between `acks=0`, `acks=1`, and `acks=all`?](#q15-what-are-the-trade-offs-between-acks0-acks1-and-acksall)
  - [Q16. How does Kafka achieve Exactly-Once Semantics (EOS)?](#q16-how-does-kafka-achieve-exactly-once-semantics-eos)
- [System Design Trade-offs](#system-design-trade-offs)
  - [Q17. Is Kafka a Message Queue or an Event Stream?](#q17-is-kafka-a-message-queue-or-an-event-stream)
  - [Q18. How does Kafka naturally prevent and handle backpressure?](#q18-how-does-kafka-naturally-prevent-and-handle-backpressure)
  - [Q19. When should you choose Kafka over RabbitMQ or traditional Pub-Sub?](#q19-when-should-you-choose-kafka-over-rabbitmq-or-traditional-pub-sub)
  - [Q20. Can you increase partition count of a topic in production? What is the catch?](#q20-can-you-increase-partition-count-of-a-topic-in-production-what-is-the-catch)

---

## Core Architecture & Concepts

### Q1. Can a partition contain events from multiple topics?

**No.** A partition belongs strictly to **exactly one topic**.

```
Topic A (cricket-events)   --> Partition 0, Partition 1
Topic B (football-events)  --> Partition 0, Partition 1
```

Partitions of `cricket-events` will never hold records from `football-events`. Each topic maintains its own isolated partition set.

---

### Q2. Do we create new partitions for every new topic?

**Yes.** When a new topic is created, Kafka provisions dedicated physical partition folders and log segment files on the designated brokers specifically for that topic.

---

### Q3. How does physical data look on disk?

On the broker host filesystem, Kafka stores partition data inside the directory configured by `log.dirs` (e.g., `/var/lib/kafka/data` or `/kafka-logs/`):

```
/kafka-logs/
  ├── orders-0/                  # Topic: orders, Partition 0
  │   ├── 00000000000000000000.log      # Actual raw message bytes
  │   ├── 00000000000000000000.index    # Sparse index: Offset -> Physical Byte Position
  │   └── 00000000000000000000.timeindex# Timestamp -> Offset lookup
  ├── orders-1/                  # Topic: orders, Partition 1
  ├── payments-0/                # Topic: payments, Partition 0
  └── payments-1/                # Topic: payments, Partition 1
```

- Each topic-partition has its own directory named `<topic-name>-<partition-id>`.
- Inside the directory, `.log` files store sequential records, while binary `.index` files allow $O(1)$ fast lookups without scanning the entire file from start to finish.

---

### Q4. What is the difference between a Topic and an Event?

| Concept | What it is | Role |
|---------|------------|------|
| **Topic** | A logical stream/channel (folder name) | Categorizes and groups related events together |
| **Event** | An immutable fact that occurred (file content) | The actual payload and metadata (`key`, `value`, `timestamp`, `headers`) |

---

### Q5. What exactly is stored inside `__consumer_offsets`?

Kafka records consumer commit positions in an internal, compacted topic named `__consumer_offsets`. It stores records with the composite structure:

$$\text{Key: } (\text{groupId}, \text{topic}, \text{partition}) \longrightarrow \text{Value: } (\text{committedOffset}, \text{metadata}, \text{timestamp})$$

Because it is a **log-compacted** topic, Kafka periodically cleans up older offset updates and preserves only the latest committed offset per consumer group and partition.

---

### Q6. Why is Kafka so fast despite writing everything to disk?

Kafka achieves high throughput on disk via four primary optimizations:
1. **Sequential I/O**: Kafka only appends to the tail of files. Sequential disk I/O on modern NVMe SSDs approaches RAM speeds by avoiding disk head movements and random seeks.
2. **OS Page Cache**: Writes hit kernel page memory first and flush asynchronously to disk via the OS background threads. Reads are served directly from RAM if consumers tail the log closely.
3. **Zero-Copy (`sendfile`)**: Data is transferred directly from the OS page cache to the network socket descriptor without being copied into application user-space memory, cutting CPU usage and context switches.
4. **End-to-End Batching**: Producers bundle records into batches (`batch.size`, `linger.ms`), reducing system calls and maximizing network packet utilization and compression efficiency.

---

## Consumers & Partition Assignment

### Q7. Can a consumer in a consumer group consume from multiple topics?

**Yes.** A consumer instance can subscribe to multiple topics simultaneously:

```java
// Explicit list of topics
consumer.subscribe(Arrays.asList("cricket-events", "football-events"));

// Or dynamic regex pattern
consumer.subscribe(Pattern.compile(".*-events"));
```

Kafka's group coordinator will balance partitions across all subscribed topics among group members.

---

### Q8. Can a consumer in a consumer group consume from multiple partitions of the same topic?

**Yes.** This is the standard behavior when the number of partitions exceeds the number of consumers in a consumer group (`partitions > consumers`).

```
Topic: tennis-events (6 partitions: P0, P1, P2, P3, P4, P5)
Consumer Group: 2 consumers (C1, C2)

Assignment:
  C1 -> P0, P1, P2
  C2 -> P3, P4, P5
```

---

### Q9. Can two consumers in the same consumer group read from the same partition?

**No.** Within the same consumer group, exactly one consumer reads from a given partition at any time.

**Why?**
If two consumers in the same group read from the same partition concurrently, offset tracking would conflict, race conditions would occur, and strictly ordered processing would be impossible to maintain.

---

### Q10. What happens if consumers in a consumer group exceed the number of partitions?

Extra consumers will sit **idle** without any partition assigned to them.

```
Topic: orders (3 partitions: P0, P1, P2)
Consumer Group: 5 consumers (C1, C2, C3, C4, C5)

Assignment:
  C1 -> P0
  C2 -> P1
  C3 -> P2
  C4 -> IDLE (No partition)
  C5 -> IDLE (No partition)
```

Idle consumers act purely as hot standbys. If `C1` crashes, Kafka immediately rebalances and assigns `P0` to `C4` or `C5`.

---

### Q11. What triggers a Consumer Group Rebalance, and what are its drawbacks?

**Triggers:**
- A new consumer joins the group.
- An existing consumer crashes or stops sending heartbeats (`session.timeout.ms`).
- A consumer takes longer than `max.poll.interval.ms` to process a fetched batch.
- Topic partitions are increased or new topics matching a regex subscription are created.

**Drawbacks:**
- **Stop-the-World Phase**: Under early rebalance protocols (Eager rebalancing), all consumers revoke partitions and stop processing until reassignment finishes.
- Modern Kafka uses **Cooperative Sticky Assignor** to incrementally reassign only the affected partitions, dramatically reducing downtime.

---

## Fault Tolerance & Guarantees

### Q12. What happens when a consumer crashes before committing its offset?

1. When a consumer crashes, the group coordinator detects the failure (missed heartbeats) and triggers a rebalance.
2. The crashed consumer's partitions are assigned to a surviving consumer in the group.
3. The new consumer reads the last committed offset from `__consumer_offsets`.
4. Because the crash happened *before* committing the processed records, the new consumer re-reads and re-processes those messages.
5. This is **at-least-once delivery**. Downstream systems must ensure **idempotent processing** (e.g., using unique database IDs or deduplication caches).

---

### Q13. Can offsets ever go backwards?

- **Under normal consumer flow**: No. Offsets in the log are strictly append-only and monotonically increasing.
- **Manual Seek**: An application can intentionally rewind its consumer cursor using:
  ```java
  consumer.seek(topicPartition, targetOffset);
  ```
  This is commonly used to replay historical data, debug faulty releases, or backfill new data stores.

---

### Q14. How does Kafka prevent data loss if a leader broker crashes?

Kafka uses **In-Sync Replicas (ISR)**:
1. Each partition has 1 Leader and $N-1$ Followers.
2. Followers continuously pull messages from the leader. Replicas that stay caught up within `replica.lag.time.max.ms` belong to the ISR set.
3. When the leader broker crashes, the Kafka Controller elects a new leader exclusively from the healthy **ISR list**.
4. By configuring `min.insync.replicas=2` and `acks=all`, writes are guaranteed to be stored across at least 2 nodes before being acknowledged, preventing data loss.

---

### Q15. What are the trade-offs between `acks=0`, `acks=1`, and `acks=all`?

| acks Setting | How it Works | Latency | Durability / Data Loss Risk |
|--------------|--------------|---------|-----------------------------|
| `acks=0` | Producer sends and moves on without waiting for response | Lowest | Highest (records lost if network drops or broker is down) |
| `acks=1` | Leader writes locally and confirms immediately | Low | Moderate (lost if leader crashes before replicas pull the record) |
| `acks=all` (`-1`) | Leader waits until all `min.insync.replicas` commit | Slightly higher | Zero data loss as long as at least one ISR replica survives |

---

### Q16. How does Kafka achieve Exactly-Once Semantics (EOS)?

Kafka achieves EOS through two foundational features:
1. **Idempotent Producer (`enable.idempotence=true`)**:
   - The broker assigns each producer a unique `Producer ID (PID)` and each batch an incremental `Sequence Number`.
   - If network retries cause duplicate sends, the broker identifies the already-committed sequence number and silently drops duplicates.
2. **Kafka Transactions (`read_committed`)**:
   - Allows atomic writes across multiple topics and partitions alongside consumer offset commits within a single atomic two-phase commit transaction.

---

## System Design Trade-offs

### Q17. Is Kafka a Message Queue or an Event Stream?

Kafka is primarily a **distributed append-only event log (event stream)**, but it can act as a message queue depending on consumer group configuration:

- **As an Event Stream**: Multiple independent consumer groups read the exact same partition concurrently without deleting data.
- **As a Message Queue**: Within a single consumer group, each record in a partition is consumed exclusively by only one consumer, acting like a point-to-point worker queue.

---

### Q18. How does Kafka naturally prevent and handle backpressure?

Traditional push brokers push messages eagerly to subscribers, causing memory bloat, blocked threads, or OOM crashes when consumers slow down.

Kafka uses a **client-driven pull model**:
- Consumers pull only the number of records they can safely process (`max.poll.records`).
- If a consumer slows down (e.g., due to downstream database locks), it simply slows down its polling rate.
- Unprocessed events accumulate safely on the broker's durable disk log up to the retention threshold, giving lagging consumers time to recover without crashing or dropping messages.

---

### Q19. When should you choose Kafka over RabbitMQ or traditional Pub-Sub?

- **Choose RabbitMQ / Pub-Sub when**:
  - You need complex routing (topic exchanges, direct routing keys, header matches).
  - You need individual message-level acknowledgments and retries.
  - You run discrete task distribution or RPC-style worker queues.
  - You do not need to replay historical messages.

- **Choose Kafka when**:
  - You need massive throughput (hundreds of thousands to millions of events/sec).
  - You need event replay, event sourcing, or audit histories.
  - You have multiple independent consumer applications reading the same event streams.
  - You need high fault tolerance and horizontal scale via partition sharding.

---

### Q20. Can you increase partition count of a topic in production? What is the catch?

**Yes, but with an ordering catch.**

- Kafka allows increasing partition count dynamically using `kafka-topics.sh --alter --partitions <new_count>`.
- **The Catch**: Keyed messages use $\text{hash}(\text{key}) \pmod N$ to route to partitions.
- When $N$ increases, the hash formula changes. Subsequent messages for key `order_123` will route to a different partition than previous messages.
- Strict chronological ordering for that specific key is broken across the boundary of partition resizing.
