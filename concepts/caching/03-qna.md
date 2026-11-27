# Caching Interview Q&A

A curated collection of practical, conceptual, HLD, and LLD questions frequently asked in system design and backend engineering interviews.

---

## Contents

- [Core Concepts & Caching Patterns](#core-concepts--caching-patterns)
  - [Q1. What is the difference between Cache-Aside and Read-Through?](#q1-what-is-the-difference-between-cache-aside-and-read-through)
  - [Q2. Why does Cache-Aside delete the key on write instead of updating it?](#q2-why-does-cache-aside-delete-the-key-on-write-instead-of-updating-it)
  - [Q3. When should you choose Write-Through vs Write-Behind (Write-Back)?](#q3-when-should-you-choose-write-through-vs-write-behind-write-back)
  - [Q4. What is Write-Around, and when is it better than Write-Through?](#q4-what-is-write-around-and-when-is-it-better-than-write-through)
  - [Q5. What is Two-Level Caching (L1 + L2), and how do you keep L1 consistent across pods?](#q5-what-is-two-level-caching-l1--l2-and-how-do-you-keep-l1-consistent-across-pods)
  - [Q6. When should you NOT use a cache?](#q6-when-should-you-not-use-a-cache)
- [Eviction, Expiration & LLD Internals](#eviction-expiration--lld-internals)
  - [Q7. How do you design an O(1) LRU Cache, and why do you need dummy Head/Tail nodes?](#q7-how-do-you-design-an-o1-lru-cache-and-why-do-you-need-dummy-headtail-nodes)
  - [Q8. How is an O(1) LFU Cache implemented, and how does it break ties?](#q8-how-is-an-o1-lfu-cache-implemented-and-how-does-it-break-ties)
  - [Q9. Why does Redis use approximated LRU instead of a true Doubly Linked List?](#q9-why-does-redis-use-approximated-lru-instead-of-a-true-doubly-linked-list)
  - [Q10. How does a cache expire keys with TTL without scanning the entire memory?](#q10-how-does-a-cache-expire-keys-with-ttl-without-scanning-the-entire-memory)
  - [Q11. How do you make an in-memory LRU Cache thread-safe without killing read throughput?](#q11-how-do-you-make-an-in-memory-lru-cache-thread-safe-without-killing-read-throughput)
- [Distributed Caching & Scaling (HLD)](#distributed-caching--scaling-hld)
  - [Q12. Redis vs Memcached: when do you pick one over the other?](#q12-redis-vs-memcached-when-do-you-pick-one-over-the-other)
  - [Q13. Why does `hash(key) % N` break when scaling a cache cluster, and how does Consistent Hashing fix it?](#q13-why-does-hashkey--n-break-when-scaling-a-cache-cluster-and-how-does-consistent-hashing-fix-it)
  - [Q14. What are Virtual Nodes in Consistent Hashing, and what problem do they solve?](#q14-what-are-virtual-nodes-in-consistent-hashing-and-what-problem-do-they-solve)
  - [Q15. How do you estimate cache memory requirements in a system design interview?](#q15-how-do-you-estimate-cache-memory-requirements-in-a-system-design-interview)
- [Production Failure Modes & Consistency](#production-failure-modes--consistency)
  - [Q16. What is a Cache Stampede (Thundering Herd), and how do you prevent it?](#q16-what-is-a-cache-stampede-thundering-herd-and-how-do-you-prevent-it)
  - [Q17. What is Cache Penetration, and how do Bloom Filters help?](#q17-what-is-cache-penetration-and-how-do-bloom-filters-help)
  - [Q18. What is a Cache Avalanche, and how is it different from a Stampede?](#q18-what-is-a-cache-avalanche-and-how-is-it-different-from-a-stampede)
  - [Q19. How do you solve the Hot Key problem when one key overloads a single Redis shard?](#q19-how-do-you-solve-the-hot-key-problem-when-one-key-overloads-a-single-redis-shard)
  - [Q20. How can a concurrent read and write cause stale data in Cache-Aside, and how do you fix it?](#q20-how-can-a-concurrent-read-and-write-cause-stale-data-in-cache-aside-and-how-do-you-fix-it)

---

## Core Concepts & Caching Patterns

### Q1. What is the difference between Cache-Aside and Read-Through?

In **Cache-Aside**, the **application** is responsible for talking to both the cache and the database. In **Read-Through**, the **cache provider** itself sits inline and fetches from the database on a miss.

| Aspect | Cache-Aside | Read-Through |
|--------|-------------|--------------|
| Who queries DB on miss? | Application code | Cache library / service (e.g., DAX, Caffeine) |
| App code complexity | Higher (`if miss -> query DB -> set cache`) | Cleaner (app only calls `cache.get(key)`) |
| Typical stack | Redis / Memcached | DynamoDB DAX, Caffeine `LoadingCache` |

---

### Q2. Why does Cache-Aside delete the key on write instead of updating it?

Two reasons:

1. **Avoids write-order race conditions:** Suppose Request A updates the DB to `V1`, then Request B updates the DB to `V2`. Due to thread scheduling, Request B updates the cache first (`V2`), and Request A updates the cache second (`V1`). The cache is now stuck with stale `V1`. Deleting the key forces the next read to fetch the latest committed value (`V2`).
2. **Avoids wasted work (lazy loading):** If a key is updated 50 times before anyone reads it, updating the cache 50 times wastes network and serialization work. Deleting once is cheaper.

---

### Q3. When should you choose Write-Through vs Write-Behind (Write-Back)?

- **Choose Write-Through** when you need **zero data loss** and **read-after-write consistency** (e.g., user profile updates, config settings). Every write goes synchronously to both cache and DB, so writes are slower, but reads are always warm and consistent.
- **Choose Write-Behind** when you have **massive write throughput** and can tolerate **eventual consistency** or minor data loss if a cache node crashes before flushing (e.g., video view counts, like counters, IoT telemetry). Multiple writes to the same key are coalesced in RAM and flushed in batches to the DB.

---

### Q4. What is Write-Around, and when is it better than Write-Through?

In **Write-Around**, writes go straight to the database and **bypass the cache** completely. Data only enters the cache when it is actually read.

It is much better than Write-Through for **write-heavy or append-only workloads** (e.g., logs, chat history, bulk imports) where newly written data is rarely read right away. Using Write-Through here would flood cache RAM with cold data and evict genuinely hot keys.

---

### Q5. What is Two-Level Caching (L1 + L2), and how do you keep L1 consistent across pods?

Two-Level Caching pairs a **local in-process cache (L1)** inside each app server (e.g., Caffeine) with a **shared distributed cache (L2)** (e.g., Redis).

- **Read flow:** Check L1 (~100 ns) -> miss -> check L2 (~1 ms) -> miss -> query DB (~15 ms).
- **Keeping L1 consistent across multiple app pods:**
  1. **Short L1 TTL:** Keep L1 TTL very small (e.g., 5 to 30 seconds) so staleness is tightly bounded.
  2. **Pub/Sub invalidation:** When a pod updates data, it updates the DB, invalidates L2 (Redis), and publishes a message on **Redis Pub/Sub** (or Kafka) telling all other app pods to evict that key from their L1 cache.

---

### Q6. When should you NOT use a cache?

1. **Strict strong-consistency paths:** Bank account balances, payment ledgers, or final inventory stock checks during checkout.
2. **Write-heavy, low-read data:** If keys are overwritten far more often than they are read, you pay invalidation cost with almost no cache hits.
3. **Unrepeatable queries:** Random search inputs or unique one-time tokens that won't be requested again.
4. **Tiny datasets:** A 5 MB table already fits in the database's own RAM buffer pool; adding an external cache only adds a network hop.

---

## Eviction, Expiration & LLD Internals

### Q7. How do you design an O(1) LRU Cache, and why do you need dummy Head/Tail nodes?

Combine two data structures:
- **`HashMap<Key, Node>`** for `O(1)` key lookup.
- **Doubly Linked List (DLL)** for `O(1)` removal and insertion at the front/back to track recency.

```
Head (Dummy) <-> [MRU Node] <-> ... <-> [LRU Node] <-> Tail (Dummy)
```

- On `get(key)`: Look up `node` in the map, unlink it from the DLL, and move it right after `Head`.
- On `put(key, val)`: Insert/update at `Head`. If capacity is full, evict `Tail.prev` and remove its key from the map.
- **Why dummy `Head` and `Tail` sentinel nodes?** Without them, every insert/remove requires `if (head == null)` and `if (node == tail)` edge-case checks. Sentinels guarantee every real node always has a non-null `prev` and `next`, making pointer logic clean and bug-free.

---

### Q8. How is an O(1) LFU Cache implemented, and how does it break ties?

LFU evicts the item with the **lowest access frequency**. If multiple keys share the same minimum frequency, it breaks ties using **LRU** (evicting the oldest-accessed key in that frequency bucket).

To do this in `O(1)`:
1. `keyToNode`: `HashMap<Key, Node>` (each node stores `key`, `value`, `freq`).
2. `freqToList`: `HashMap<Integer, DoublyLinkedList>` (one DLL per frequency count).
3. `minFreq`: integer tracking the lowest active frequency.

Whenever a key is accessed, it moves from `freqToList[f]` to the front of `freqToList[f + 1]`. If `freqToList[minFreq]` becomes empty, `minFreq` increments by 1. On eviction, remove the tail of `freqToList[minFreq]`.

---

### Q9. Why does Redis use approximated LRU instead of a true Doubly Linked List?

A true Doubly Linked List requires **16 bytes of extra pointer overhead (`prev` + `next`) per key** (on 64-bit systems), plus pointer-chasing work on every single read.

Instead, Redis stores a compact **24-bit LRU timestamp** inside every object header and runs **approximated LRU**: when memory is full, it randomly samples a small pool of keys (default `maxmemory-samples 5`) and evicts the oldest one among the sample. Increasing `maxmemory-samples` to 10 approaches true LRU accuracy while saving gigabytes of RAM at scale.

---

### Q10. How does a cache expire keys with TTL without scanning the entire memory?

Scanning millions of keys on every tick would freeze the cache. Systems like Redis combine two strategies:

1. **Passive (Lazy) Expiration:** Check `now > expiresAt` only when a client actually requests that key. If expired, delete it and return a miss.
2. **Active Expiration:** A background job periodically samples a small random batch of keys that have TTLs (e.g., 20 keys every 100 ms), deletes the expired ones, and repeats immediately if more than 25% of the sample was expired.

---

### Q11. How do you make an in-memory LRU Cache thread-safe without killing read throughput?

In an LRU cache, even `get()` mutates the Doubly Linked List (moving the node to `Head`), so a `ReadWriteLock` won't allow concurrent reads on the same list.

**Production-grade solution: Striped Locking (Sharded Cache)**
- Partition the cache into `N` independent segments (e.g., 16 or 64 shards using `hash(key) % N`).
- Each shard has its own `HashMap`, `DoublyLinkedList`, and `ReentrantLock`.
- Threads reading/writing different keys hit different locks, reducing contention by `N`x. (High-performance libraries like **Caffeine** go further by buffering reads in a lock-free ring buffer and updating eviction metadata asynchronously.)

---

## Distributed Caching & Scaling (HLD)

### Q12. Redis vs Memcached: when do you pick one over the other?

| Dimension | Redis | Memcached |
|-----------|-------|-----------|
| **Data types** | Strings, Hashes, Lists, Sets, Sorted Sets, Streams, HyperLogLog | Raw Key-Value bytes/strings only |
| **Threading** | Single-threaded command execution (with I/O threads in 6+) | Multi-threaded across CPU cores |
| **Persistence** | Yes (RDB snapshots + AOF) | No (pure volatile memory) |
| **HA & Clustering** | Built-in Replication, Sentinel, Redis Cluster | Client-side consistent hashing only |

**Answer:** Pick **Redis** by default for rich data structures (leaderboards with `ZSET`, rate limiters, distributed locks), persistence, and built-in clustering. Pick **Memcached** only when you need a pure, volatile, multi-threaded string/object cache for massive static payloads.

---

### Q13. Why does `hash(key) % N` break when scaling a cache cluster, and how does Consistent Hashing fix it?

With `hash(key) % N`, changing `N` (adding a node from 3 -> 4, or losing a node from 3 -> 2) changes the modulo result for **almost every key** (~75% to 80% of keys map to a new server). That triggers a sudden storm of cache misses against the database.

**Consistent Hashing** maps both nodes and keys onto a `0` to `2^32 - 1` hash ring and routes each key clockwise to the next node. When a node is added or removed, only **`1 / N`** of the keys (those in that node's slice of the ring) move, leaving all other keys untouched.

---

### Q14. What are Virtual Nodes in Consistent Hashing, and what problem do they solve?

With only a few physical servers (e.g., 3 nodes) on a hash ring, random hashing can place them unevenly, giving one node 70% of the ring (**data skew**). Worse, if one node crashes, its entire load dumps onto a single clockwise neighbor, potentially causing a cascading failure.

**Virtual Nodes (vNodes)** assign each physical server **100 to 200 positions** around the ring (e.g., `NodeA#1`, `NodeA#2`, ...). This guarantees uniform key distribution and ensures that if a server dies, its keys spread evenly across **all** surviving nodes.

---

### Q15. How do you estimate cache memory requirements in a system design interview?

Use the **80/20 rule (working set)**:

1. **Total daily items / reads:** Suppose a URL shortener or social app serves `100M` active items/day.
2. **Hot working set (20%):** Caching the hottest 20% = `20M` items.
3. **Average payload size:** Suppose each cached item (key + serialized value) is `500 bytes`.
4. **RAM calculation:** `20,000,000 * 500 bytes = 10 GB`.
5. **Overhead buffer (+20-30%):** Account for Redis hash table/metadata overhead -> `~12 to 13 GB` of RAM (easily fits on a single `16 GB` or `32 GB` node, or 2 shards with replicas for HA).

---

## Production Failure Modes & Consistency

### Q16. What is a Cache Stampede (Thundering Herd), and how do you prevent it?

A **Cache Stampede** happens when a single ultra-hot key expires and thousands of concurrent requests miss the cache at the exact same moment, all querying the database simultaneously.

**Fixes:**
1. **Singleflight / Mutex Lock:** Only the first request acquires a lock (e.g., `SETNX` or Go's `singleflight`), queries the DB, and populates the cache while the other requests wait for that single result.
2. **Refresh-Ahead / Logical Expiration:** Refresh hot keys asynchronously in the background before their TTL actually expires.

---

### Q17. What is Cache Penetration, and how do Bloom Filters help?

**Cache Penetration** happens when requests repeatedly ask for keys that **do not exist in either the cache or the database** (e.g., `GET /user/-1` from a bug or DDoS attack). Every request bypasses the cache and hits the DB.

**Fixes:**
1. **Cache Null Markers:** Store `key -> NULL` in the cache with a short TTL (e.g., 30 to 60 seconds).
2. **Bloom Filter:** Place a space-efficient **Bloom Filter** in front of the cache containing all valid IDs. If the Bloom Filter says *"definitely not present"*, reject the request immediately in `O(1)` without touching Redis or the DB.

---

### Q18. What is a Cache Avalanche, and how is it different from a Stampede?

- **Cache Stampede:** **One** super-hot key expires and triggers a DB spike.
- **Cache Avalanche:** **Thousands of keys** expire at the exact same time (because they were preloaded together with the same fixed TTL), or an entire cache cluster goes down, slamming the DB with massive traffic.

**Fixes for Avalanche:**
- Add **TTL Jitter**: `ttl = base_ttl + random(0, 300s)` so expirations spread out over time.
- Use **Redis Cluster / Sentinel** for High Availability, plus circuit breakers / rate limiters in front of the DB.

---

### Q19. How do you solve the Hot Key problem when one key overloads a single Redis shard?

Even in a 10-shard Redis Cluster, Consistent Hashing routes a single viral key (e.g., a celebrity's live stream metadata at 500K QPS) to **one single shard**, saturating its CPU and network NIC.

**Fixes:**
1. **L1 Local In-Memory Cache:** Cache the hot key in each app server's own RAM for a tiny TTL (e.g., 1 to 5 seconds) to absorb 99% of reads locally.
2. **Key Salting / Replication:** Store copies like `hot_key#1` through `hot_key#10` across all 10 Redis shards and have readers pick a random suffix.

---

### Q20. How can a concurrent read and write cause stale data in Cache-Aside, and how do you fix it?

**The Race Condition:**
1. Reader A misses the cache and reads old value `V1` from the DB.
2. Writer B updates the DB to `V2` and deletes the key from the cache.
3. Reader A (which was briefly stalled) now writes stale `V1` into the cache!

**Fixes:**
1. **Always set a TTL:** Bounds how long stale data can ever survive.
2. **Delayed Double Delete:** Writer B deletes the cache, updates the DB, sleeps briefly (~100-500 ms), and deletes the cache a second time to wipe out any slow in-flight read.
3. **CDC (Change Data Capture):** Stream DB WAL/binlog events (via Debezium + Kafka) to an invalidation worker, decoupling cache updates from app-level race conditions.
