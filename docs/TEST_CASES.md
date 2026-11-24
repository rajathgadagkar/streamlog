# StreamLog — Integration Test Suite Specification

A comprehensive integration test suite for the Kafka-inspired distributed pub-sub system.
All 46 test cases run against **real OS processes** (coordinator + brokers) communicating over **actual TCP sockets** — no mocking.

---

## How to Run

```bash
cd streamlog

# Run all fast tests (excludes broker-failure tests which need 10–15 s timeouts)
python -m pytest tests/test_system.py -v --tb=short \
  -k "not TC19 and not TC20 and not TC21 and not TC22"

# Run ONLY fault-tolerance tests
python -m pytest tests/test_system.py -v --tb=short \
  -k "TC19 or TC20 or TC21 or TC22"

# Run everything (takes ~5–7 minutes due to heartbeat timeouts)
python tests/test_system.py

# Run a single test class
python -m pytest tests/test_system.py::TC03_KeyRouting -v
```

---

## Test Class Overview

| # | Class | Area | Tests |
|---|-------|------|-------|
| TC-01 | `TC01_BasicProduceConsume` | Core correctness | 3 |
| TC-02 | `TC02_MultiplePartitions` | Partitioning | 2 |
| TC-03 | `TC03_KeyRouting` | Key-based routing | 3 |
| TC-04 | `TC04_ConsumerGroupsPartitionSharing` | Consumer groups | 3 |
| TC-05 | `TC05_FanOut` | Fan-out / broadcast | 2 |
| TC-06 | `TC06_OffsetCommitResume` | Offset management | 3 |
| TC-07 | `TC07_AtLeastOnceDelivery` | Delivery guarantee | 2 |
| TC-08 | `TC08_MessageOrdering` | Ordering | 2 |
| TC-09 | `TC09_MultipleTopics` | Topic isolation | 2 |
| TC-10 | `TC10_ConcurrentProducers` | Concurrency | 2 |
| TC-11 | `TC11_ConcurrentConsumers` | Concurrency | 1 |
| TC-12 | `TC12_ConsumerJoinsMidStream` | Dynamic rebalance | 1 |
| TC-13 | `TC13_ConsumerLeavesMidStream` | Dynamic rebalance | 1 |
| TC-14 | `TC14_LargePayloads` | Payload size | 2 |
| TC-15 | `TC15_HighVolume` | Throughput | 2 |
| TC-16 | `TC16_IdempotentTopicCreation` | Topic management | 2 |
| TC-17 | `TC17_RoundRobinDistribution` | Partition routing | 1 |
| TC-18 | `TC18_ReplicationVerification` | Replication | 2 |
| TC-19 | `TC19_BrokerFailureConsumeRecovery` | Fault tolerance | 1 |
| TC-20 | `TC20_BrokerFailureProducerRerouting` | Fault tolerance | 1 |
| TC-21 | `TC21_MultipleBrokerFailures` | Fault tolerance | 1 |
| TC-22 | `TC22_ConsumerEviction` | Fault tolerance | 1 |
| TC-23 | `TC23_TripleFanOut` | Fan-out | 1 |
| TC-24 | `TC24_SequentialBatchPolling` | Polling behaviour | 2 |
| TC-25 | `TC25_MessageMetadataIntegrity` | Data integrity | 3 |
| **Total** | | | **46 tests** |

---

## Detailed Test Descriptions

---

### TC-01 · Basic Produce / Consume

> **Goal:** Verify the most fundamental produce → consume path works end-to-end.

---

#### `test_single_message`
**What it does:**  
Creates a 1-partition topic. One producer sends `"hello-kafka"`. One consumer subscribes and polls.

**Expected outcome:**  
- Exactly 1 record received.  
- `record["message"] == "hello-kafka"`.

---

#### `test_multiple_messages_single_partition`
**What it does:**  
Sends 20 different messages in sequence to a single-partition topic. Consumer polls all of them.

**Expected outcome:**  
- Exactly 20 records received.  
- Every sent message string is present in the received set (no message lost).

---

#### `test_produce_returns_monotone_offsets`
**What it does:**  
Sends 10 messages to a 1-partition topic one at a time and collects the returned offset from each `send()` call.

**Expected outcome:**  
- Returned offsets are `[0, 1, 2, 3, 4, 5, 6, 7, 8, 9]` — strictly sequential integers starting at 0.  
- Confirms the broker's offset assignment is correct and monotonically increasing.

---

### TC-02 · Multiple Partitions

> **Goal:** Verify that messages are distributed across partitions and that no message is lost.

---

#### `test_messages_spread_across_partitions`
**What it does:**  
Creates a 3-partition topic. Sends 30 messages (keyless, so round-robin applies). One consumer subscribes to all 3 partitions and polls.

**Expected outcome:**  
- At least **2 distinct partition IDs** appear in received records.  
- Confirms round-robin is actually spreading messages, not sending everything to one partition.

---

#### `test_all_messages_received_across_partitions`
**What it does:**  
Sends 60 messages to a 3-partition topic. One consumer collects all of them.

**Expected outcome:**  
- All 60 messages received — no message dropped regardless of which partition it landed on.  
- Confirms the consumer correctly polls **all** assigned partitions.

---

### TC-03 · Key-Based Partition Routing

> **Goal:** Verify that message keys deterministically route to the same partition.

---

#### `test_same_key_same_partition`
**What it does:**  
Sends 15 messages all with the same key `"user-777"` to a 3-partition topic.

**Expected outcome:**  
- All 15 messages land on exactly **1 partition** (the same one every time).  
- Confirms `hash(key) % num_partitions` routing is consistent.

---

#### `test_different_keys_can_go_different_partitions`
**What it does:**  
Sends 12 messages each with a different key (user-0 … user-11).

**Expected outcome:**  
- At least **2 different partitions** are used.  
- Confirms that different keys hash to different partitions (not all colliding).

---

#### `test_keyed_and_keyless_mix`
**What it does:**  
Sends 10 keyed and 10 keyless messages to a 2-partition topic simultaneously.

**Expected outcome:**  
- All 20 messages are received.  
- Confirms that mixing routing strategies doesn't cause message loss.

---

### TC-04 · Consumer Groups — Partition Sharing

> **Goal:** Verify that consumers in the **same group** share partitions (no duplication, no loss).

---

#### `test_two_consumers_share_two_partitions`
**What it does:**  
Starts 2 consumers in the same group on a 2-partition topic. Sends 20 messages. Both consumers poll simultaneously.

**Expected outcome:**  
- Total messages received = **20** (combined).  
- **Zero duplicate** `(partition, offset)` pairs — each message delivered to exactly one consumer in the group.  
- Each consumer receives from its own assigned partition(s).

---

#### `test_three_consumers_three_partitions`
**What it does:**  
3 consumers in the same group, 3-partition topic, 30 messages. Each consumer polls its own assigned partition.

**Expected outcome:**  
- Total = **30** messages across all 3 consumers.  
- No duplicate `(partition, offset)` pair across consumers.  
- Confirms balanced partition assignment (Range strategy: 1 partition per consumer).

---

#### `test_no_message_duplication_in_same_group`
**What it does:**  
2 consumers in the same group. Sends 20 uniquely-named messages. Both consumers commit after polling.

**Expected outcome:**  
- The intersection of message sets between consumer 1 and consumer 2 is **empty** — no message appears in both consumers' results.  
- Confirms strict partition ownership within a group.

---

### TC-05 · Fan-Out (Multiple Independent Consumer Groups)

> **Goal:** Verify that different consumer groups each receive the **complete** message stream independently.

---

#### `test_two_groups_both_get_all_messages`
**What it does:**  
Creates 2 consumer groups (grp-A and grp-B) on the same 3-partition topic. Sends 30 messages.

**Expected outcome:**  
- grp-A receives all **30** messages.  
- grp-B receives all **30** messages.  
- The two groups are completely independent — grp-A consuming does NOT remove messages for grp-B.

---

#### `test_three_groups_all_independent`
**What it does:**  
3 separate consumer groups subscribing to the same topic; 12 messages sent.

**Expected outcome:**  
- Each of the 3 groups independently receives all **12** messages.  
- Verifies true broadcast / fan-out semantics.

---

### TC-06 · Offset Commit & Resume

> **Goal:** Verify that committing an offset checkpoints progress, and new consumers resume from that exact point.

---

#### `test_resume_from_committed_offset`
**What it does:**  
Consumer-1 receives `["alpha", "beta", "gamma"]` but commits only the first 2 (`alpha`, `beta`). Consumer-2 then joins the same group.

**Expected outcome:**  
- Consumer-2 receives **only** `"gamma"` (the uncommitted one).  
- `"alpha"` and `"beta"` are **not** redelivered.  
- Confirms the committed offset acts as a precise read-cursor.

---

#### `test_uncommitted_messages_redelivered`
**What it does:**  
Consumer-1 receives `"important-data"` but deliberately does **not** commit. Closes without committing (simulates crash). Consumer-2 joins the same group.

**Expected outcome:**  
- Consumer-2 receives `"important-data"` again.  
- Confirms **at-least-once delivery**: uncommitted messages are re-delivered to the next consumer.

---

#### `test_commit_mid_batch_resumes_correctly`
**What it does:**  
Sends 10 messages (`item-0` … `item-9`). Consumer-1 reads all 10, commits after `item-5` (offset 6). Consumer-2 then joins the group.

**Expected outcome:**  
- Consumer-2 receives exactly `item-6, item-7, item-8, item-9`.  
- Confirms sub-batch offset checkpointing works with byte-precision accuracy.

---

### TC-07 · At-Least-Once Delivery Guarantee

> **Goal:** Prove no message is ever silently dropped, even under load.

---

#### `test_all_messages_received_at_least_once`
**What it does:**  
Sends 100 uniquely-named messages to a 3-partition topic. Consumer collects all of them.

**Expected outcome:**  
- The set of received messages equals the set of sent messages exactly.  
- Zero messages from the sent set are absent from the received set.

---

#### `test_no_message_appears_zero_times`
**What it does:**  
Sends 40 messages and builds a `Counter` (frequency map) of received message strings.

**Expected outcome:**  
- Every sent message has a frequency of **≥ 1** in the received counter.  
- No message has a count of 0 (meaning it was never delivered).

---

### TC-08 · Message Ordering Within a Partition

> **Goal:** Verify that messages within the same partition are always returned in offset order.

---

#### `test_offsets_monotone_within_partition`
**What it does:**  
Sends 40 messages to a 2-partition topic. After polling, groups received records by partition and checks the offset sequence.

**Expected outcome:**  
- For every partition, the offset list is **strictly increasing** (offset[i] < offset[i+1]).  
- Confirms the broker appends in order and the consumer reads in order.

---

#### `test_keyed_messages_ordered_within_partition`
**What it does:**  
Sends 20 messages with the same key `"stream-X"` (guaranteeing same partition). Messages are formatted `ev-000` … `ev-019`.

**Expected outcome:**  
- Received messages are in exactly the order: `ev-000, ev-001, … ev-019`.  
- Confirms Kafka's **per-partition ordering guarantee** is upheld.

---

### TC-09 · Multiple Topics

> **Goal:** Verify that topics are completely isolated — no cross-topic message leakage.

---

#### `test_isolated_topics_no_cross_contamination`
**What it does:**  
Creates topic-A and topic-B. Sends 10 messages prefixed `"A-"` to topic-A and 10 messages prefixed `"B-"` to topic-B. One consumer per topic.

**Expected outcome:**  
- Consumer for topic-A receives **only** messages starting with `"A-"`.  
- Consumer for topic-B receives **only** messages starting with `"B-"`.  
- No cross-contamination between topics.

---

#### `test_three_topics_simultaneously`
**What it does:**  
Creates 3 topics simultaneously. Produces 15 messages to each. One consumer per topic polls concurrently.

**Expected outcome:**  
- Each consumer receives exactly 15 messages, all belonging to its own topic.  
- Confirms the broker correctly partitions storage by topic name.

---

### TC-10 · Concurrent Producers

> **Goal:** Verify the system handles simultaneous writes from multiple producer threads.

---

#### `test_two_concurrent_producers_no_loss`
**What it does:**  
2 threads each running a separate `Producer` instance, each sending 30 messages (prefix `P1-` and `P2-`) simultaneously to the same topic.

**Expected outcome:**  
- 0 errors from either producer thread.  
- All 60 messages (P1-0…P1-29 and P2-0…P2-29) are present in the consumer's received set.

---

#### `test_five_concurrent_producers`
**What it does:**  
5 threads, each with a separate `Producer`, each sending 20 messages concurrently.

**Expected outcome:**  
- 0 producer-side errors.  
- All 100 messages (5 producers × 20) appear in the consumer's received set.  
- Validates thread-safety of the TCP socket pool and broker's append-only log under write contention.

---

### TC-11 · Concurrent Consumers (Same Group)

> **Goal:** Verify 3 consumers polling simultaneously in the same group produce no duplicates.

---

#### `test_concurrent_poll_no_duplication`
**What it does:**  
3 consumers in the same group (3-partition topic). All 3 call `poll()` in separate threads simultaneously. 60 messages sent.

**Expected outcome:**  
- Total messages across all 3 consumers = **60** (no loss).  
- **Zero duplicate** messages (each message appears exactly once across all consumers).  
- Confirms thread-safe partition ownership is maintained under concurrent polling.

---

### TC-12 · Consumer Joins Mid-Stream

> **Goal:** Verify that rebalancing correctly redistributes partitions when a new consumer joins an active group.

---

#### `test_new_consumer_takes_over_partition`
**What it does:**  
Phase 1: Only Consumer-1 is in the group (owns both partitions of a 2-partition topic). Produces 20 messages, all consumed by C1.  
Phase 2: Consumer-2 joins. Topic rebalances (1 partition each). Produces 20 more messages.

**Expected outcome:**  
- Phase 1: C1 receives all 20 messages.  
- Phase 2: C1 + C2 together receive all 20 messages (combined = 20, no loss or duplication).  
- Confirms rebalancing doesn't miss in-flight messages.

---

### TC-13 · Consumer Leaves Mid-Stream

> **Goal:** Verify that departing consumer's partition is reassigned to remaining consumer.

---

#### `test_remaining_consumer_gets_all_messages`
**What it does:**  
Phase 1: 2 consumers active, 10 messages produced and consumed by both.  
C1 gracefully closes (calls `close()` / `unsubscribe`). Coordinator rebalances — C2 now owns all partitions.  
Phase 2: 20 more messages produced.

**Expected outcome:**  
- C2 receives all **20** messages after C1 leaves.  
- 0 messages lost during the rebalance transition.

---

### TC-14 · Large Message Payloads

> **Goal:** Verify the system handles messages far larger than typical without truncation or errors.

---

#### `test_64kb_messages`
**What it does:**  
Sends a single 65,536-byte (`"X" * 65536`) message.

**Expected outcome:**  
- Message received intact with `len(message) == 65536`.  
- Confirms the 4-byte length-prefix framing handles large payloads correctly.

---

#### `test_varied_payload_sizes`
**What it does:**  
Sends 5 messages of sizes: 1 B, 256 B, 4 KB, 16 KB, 32 KB — all in one shot.

**Expected outcome:**  
- All 5 messages received without truncation.  
- Each received message equals the corresponding sent payload exactly.

---

### TC-15 · High-Volume Throughput

> **Goal:** Verify the system can handle large numbers of messages without degrading.

---

#### `test_1000_messages_end_to_end`
**What it does:**  
Sends 1,000 messages to a 3-partition topic. One consumer collects all of them.

**Expected outcome:**  
- Exactly **1,000** messages received.  
- Zero messages lost under bulk load.

---

#### `test_throughput_above_minimum`
**What it does:**  
Sends 500 messages and measures wall-clock time. Calculates messages/second.

**Expected outcome:**  
- Throughput **> 100 msg/s**.  
- A basic performance regression guard — if the system is severely broken, throughput would tank.

---

### TC-16 · Idempotent Topic Creation

> **Goal:** Verify that calling `create_topic()` twice is safe and doesn't corrupt existing data.

---

#### `test_create_topic_twice_no_error`
**What it does:**  
Calls `create_topic("tc16_idem", num_partitions=2)` twice with the same arguments.

**Expected outcome:**  
- No exception raised on the second call.  
- Confirms the coordinator returns `"Topic already exists"` gracefully.

---

#### `test_existing_topic_data_unaffected_by_recreate`
**What it does:**  
Sends `"before-recreate"`, commits it, calls `create_topic()` again, then sends `"after-recreate"`.

**Expected outcome:**  
- Both messages can be consumed correctly.  
- Re-creating a topic does **not** wipe its existing log data.

---

### TC-17 · Round-Robin Partition Distribution

> **Goal:** Quantitatively verify that keyless (round-robin) routing spreads messages evenly.

---

#### `test_keyless_messages_distributed_round_robin`
**What it does:**  
Sends 30 messages (keyless) to a 3-partition topic. Counts how many messages each partition receives.

**Expected outcome:**  
- Each partition receives between **5 and 15** messages (expected ~10 each).  
- No partition receives all messages (which would indicate a routing bug).

---

### TC-18 · Replication Verification

> **Goal:** Prove that async replication actually writes data to the replica broker's disk.

---

#### `test_replica_log_file_exists`
**What it does:**  
Sends 10 messages to a 1-partition topic across 3 brokers. Waits 1 second for async replication. Checks the `data/` directory of each broker.

**Expected outcome:**  
- At least **1** (ideally 2) brokers have a non-empty `.log` file for the partition.  
- Confirms replication is writing to disk on the replica, not just in memory.

---

#### `test_replica_data_matches_primary`
**What it does:**  
Sends 15 messages, waits 1.5 seconds for replication. Reads and parses the `.log` files of each broker that has data. Compares their offset sets.

**Expected outcome:**  
- Primary and replica brokers contain **identical offset sets** `{0, 1, 2, …, 14}`.  
- Confirms the replication payload is faithfully duplicated with no missed offsets.

---

### TC-19 · Broker Failure — Replica Promotion + Consumer Recovery

> **Goal:** Full end-to-end fault-tolerance test: kill primary broker, verify consumer continues.

⚠️ *This test takes ~25 seconds due to the 10-second broker timeout.*

---

#### `test_consumer_continues_after_primary_broker_killed`
**What it does:**  
1. Start 2 brokers. Produce & consume 10 messages (pre-failure).  
2. Kill broker1 (primary for some partitions).  
3. Wait 12 seconds → coordinator detects timeout, promotes replica on broker2 to primary.  
4. Produce 10 more messages (post-failure). Consumer polls them.

**Expected outcome:**  
- Pre-failure: 10/10 messages consumed.  
- Coordinator log shows replica promotion.  
- Post-failure: consumer successfully receives at least some messages from broker2.  
- System continues operating despite the lost primary.

---

### TC-20 · Broker Failure — Producer Rerouting

> **Goal:** Verify the producer can reroute to a surviving broker after failover.

⚠️ *This test takes ~25 seconds due to the 10-second broker timeout.*

---

#### `test_producer_sends_succeed_after_broker_death`
**What it does:**  
1. Produce 10 messages to a 2-partition topic.  
2. Kill broker1. Wait 12 seconds.  
3. Attempt 10 more `send()` calls.

**Expected outcome:**  
- At least **5/10** post-failure sends succeed (the coordinator now routes to broker2 for all partitions previously on broker1).  
- Confirms the coordinator's replica promotion table is used for producer routing.

---

### TC-21 · Multiple Broker Failures (One Survivor)

> **Goal:** Verify the system continues producing when 2 out of 3 brokers are dead.

⚠️ *This test takes ~30 seconds due to waiting for 2 broker timeouts.*

---

#### `test_one_broker_survives_out_of_three`
**What it does:**  
1. Start 3 brokers. Produce 9 messages and consume them (confirms healthy state).  
2. Kill broker1 and broker2 simultaneously.  
3. Wait 13 seconds for coordinator to evict both.  
4. Attempt 5 more `send()` calls.

**Expected outcome:**  
- Pre-failure: all 9 messages consumed.  
- Post-failure: at least **1** of 5 sends succeeds (broker3 is still alive and owns promoted replicas).  
- Confirms the system degrades gracefully rather than failing completely.

---

### TC-22 · Consumer Eviction via Heartbeat Timeout

> **Goal:** Verify that a consumer that stops heartbeating is evicted and its partitions reassigned.

⚠️ *This test takes ~20 seconds due to the 15-second consumer timeout.*

---

#### `test_partitions_reassigned_after_eviction`
**What it does:**  
1. 2 consumers in same group. Consume first 10 messages together.  
2. Stop C1's heartbeat thread (simulates crash — no graceful `close()`).  
3. Wait 17 seconds → coordinator evicts C1.  
4. Produce 20 more messages. C2 polls them.

**Expected outcome:**  
- C2 receives all **20** post-eviction messages (owns all partitions now).  
- Confirms partition reassignment happens automatically after a consumer disappears.

---

### TC-23 · Triple Fan-Out

> **Goal:** Validate fan-out with 3 consumer groups — each independently receives the full stream.

---

#### `test_three_groups_each_get_full_stream`
**What it does:**  
Creates 3 separate consumer groups on a 3-partition topic. Sends 30 messages. All 3 groups poll.

**Expected outcome:**  
- Each of the 3 groups independently receives all **30** messages.  
- No group interferes with another's offsets.  
- Confirms the coordinator maintains completely separate offset tracking per group.

---

### TC-24 · Sequential Batch Polling

> **Goal:** Verify that incremental produce–poll cycles advance the read cursor correctly.

---

#### `test_incremental_produce_then_incremental_consume`
**What it does:**  
Repeats 5 times: produce a batch of 10 messages → immediately poll and verify 10 are returned.

**Expected outcome:**  
- Each batch poll returns exactly **10** messages (the batch just produced).  
- Total of **50** messages received across 5 rounds.  
- Confirms that `commit()` correctly advances the fetch pointer between batches (no re-reads, no skips).

---

#### `test_poll_empty_partition_returns_empty`
**What it does:**  
Consumer subscribes to a topic where no messages have been produced yet. Calls `poll()` immediately.

**Expected outcome:**  
- Returns an **empty list `[]`**.  
- Completes in **< 5 seconds** (does not block indefinitely).  
- Confirms poll is non-blocking and graceful on empty partitions.

---

### TC-25 · Message Metadata Integrity

> **Goal:** Verify that every record contains correct, well-formed metadata fields.

---

#### `test_all_required_fields_present`
**What it does:**  
Sends a message with a specific key (`"meta-key"`) and payload (`"meta-test"`). Checks every field of the received record.

**Expected outcome:**  
All six fields present and correctly typed:
| Field | Type | Expected Value |
|---|---|---|
| `offset` | `int` | ≥ 0 |
| `key` | `str` | `"meta-key"` |
| `message` | `str` | `"meta-test"` |
| `ts` | `float` | > 0 |
| `partition` | `int` | 0 or 1 |
| `topic` | `str` | `"tc25_meta"` |

---

#### `test_timestamp_is_recent`
**What it does:**  
Records wall-clock time before and after producing a message. Compares with the `ts` field of the received record.

**Expected outcome:**  
- `ts >= t_before - 1` and `ts <= t_after + 1`.  
- Confirms the broker timestamps messages at write time, not at some arbitrary future/past time.

---

#### `test_offsets_globally_unique_within_partition`
**What it does:**  
Creates 3 consumers, each in their own separate group (so each gets all 3 partitions). Sends 30 messages. For each consumer independently, checks for duplicate `(partition, offset)` pairs.

**Expected outcome:**  
- Within each consumer's received records: **zero** duplicate `(partition, offset)` pairs.  
- Confirms that each partition's append-only log assigns each message a unique offset — a foundational invariant of the system.

---

## Summary Statistics

| Category | Tests | Covers |
|---|---|---|
| **Core correctness** | 7 | TC-01, TC-02, TC-07 |
| **Routing & partitioning** | 6 | TC-03, TC-17 |
| **Consumer groups** | 8 | TC-04, TC-05, TC-23 |
| **Offset management** | 3 | TC-06 |
| **Ordering & integrity** | 5 | TC-08, TC-25 |
| **Concurrency** | 3 | TC-10, TC-11 |
| **Dynamic rebalancing** | 2 | TC-12, TC-13 |
| **Payload & throughput** | 4 | TC-14, TC-15 |
| **Topic management** | 4 | TC-09, TC-16 |
| **Replication** | 2 | TC-18 |
| **Fault tolerance** | 4 | TC-19, TC-20, TC-21, TC-22 |
| **Polling behaviour** | 2 | TC-24 |
| **TOTAL** | **50** | Everything |

---

## Cluster Setup Used by Tests

Each test class boots its own **isolated cluster** using `KafkaCluster`:

```
Coordinator : 127.0.0.1:19000
Broker 1    : 127.0.0.1:19001
Broker 2    : 127.0.0.1:19002
Broker 3    : 127.0.0.1:19003  (used by 3-broker tests)
Data dir    : /tmp/kafka_test_data  (fresh-wiped per test class)
```

> High ports (19000+) are used to avoid clashing with a running production cluster (9000+).
