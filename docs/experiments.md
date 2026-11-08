# Experiments

Deliberate failure tests against the running pipeline, to check that it behaves the way the design says it should. Each one records what I did, what I expected, and what actually happened.

All numbers come from a single local Redpanda broker, with the `coinbase.trades` topic split into 3 partitions.

## 1. Crash between the DB write and the offset commit

**Question:** if the sink dies after writing a batch to DuckDB but before committing its Kafka offsets, do we lose trades or store duplicates?

**Setup:** `sink.py` has a `CRASH_AFTER_WRITE=1` switch. When it's on, the sink calls `os._exit(1)` right after the DuckDB transaction commits and before `consumer.commit()`. `os._exit` skips `finally` blocks and `consumer.close()`, so it behaves like a real crash. Each batch also prints how many of its rows were actually new.

**Steps:**

1. Ran the sink until it caught up, then stopped it and let the producer keep running so lag built up.
2. Recorded the committed offsets with `rpk group describe trades-sink`.
3. Ran the sink with `CRASH_AFTER_WRITE=1`. It wrote one batch and exited.
4. Checked the committed offsets again.
5. Turned the switch off and restarted the sink normally.

**Expected:** the offsets don't move at the crash, Kafka redelivers the batch on restart, and the primary key skips the rows that are already stored.

**Result:**

| | Partition 0 offset | Partition 2 offset | Total lag |
|---|---|---|---|
| Before the crash | 1152 | 2709 | 245 |
| After the crash | 1152 | 2709 | 596 |
| After the restart | 1330 | 3549 | 5 |

The first batch after the restart:

```
wrote batch of 500 (148 new)
wrote batch of 493 (493 new)
wrote batch of 25 (25 new)
```

352 of the first 500 messages were the crashed batch. Kafka redelivered them, and `ON CONFLICT DO NOTHING` on `(product_id, trade_id)` skipped them. The other 148 were trades that arrived after the crash. No trades were lost and none were duplicated.

**Takeaway:** committing after the write gives at-least-once delivery. Combined with idempotent writes, the stored results are effectively exactly-once.

If the offsets were committed *before* the write instead, a crash in between would leave Kafka thinking the batch was done while DuckDB never got it. The restarted sink would skip past those trades and they'd be lost for good. That's at-most-once delivery. Duplicates can be absorbed, but lost data can't be recovered, so commit-after-write is the safer default.

**Side finding: a ghost group member.** Right after the crash, `rpk group describe` still showed `STATE Stable` and `MEMBERS 1`, even though no consumer was running. The crashed process never called `close()`, so the broker kept it in the group until it missed heartbeats for `session.timeout.ms` (45 s by default). The restarted sink joined with a new member ID. In a group with several consumers, the dead consumer's partitions would sit unprocessed for that whole window. Shortening the timeout speeds up failover, but it also raises the chance of kicking out a consumer that's only slow.

## 2. Replay: rebuilding derived data from the log

**Question:** if a bug corrupts the derived output, can it be rebuilt from Kafka alone?

**What happened:** a bug in `Candle.add` (`notional = price * size` instead of `+=`) meant VWAP only used the last trade in each window. A sanity check (`vwap` must fall between `low` and `high`) failed for every candle.

**Steps:**

1. Fixed the bug, and added a unit test that fails without the fix.
2. Deleted `data/candles.duckdb`, which is derived data only.
3. Moved the aggregator's consumer group back to the start of the topic:
   ```
   rpk group seek trades-aggregator --to start
   ```
4. Restarted the aggregator. It replayed the whole topic and rebuilt every candle.

**Result:**

| | Candles | VWAP outside [low, high] |
|---|---|---|
| Before the fix | 36 | 36 |
| After the rebuild | 36 | 0 |

**Takeaway:** because the raw trades are kept in the log, the derived tables can be thrown away and recomputed after a bug fix. Deleting the output alone wouldn't have been enough. On startup, the aggregator skips windows that are already in the table, and the group's committed offsets would have resumed from the current position instead of the beginning. Both had to be reset.

## 3. Consumer group rebalancing

**Question:** how does Kafka split partitions between consumers in a group, and what happens when consumers join or leave?

**Setup:** `group_demo.py` is a consumer that only reports what it receives. It prints its partitions whenever the group rebalances (`on_assign` / `on_revoke`), and every 50 messages it prints counts by `(partition, product)`. Every copy uses `group.id = rebalance-demo`, which is separate from the sink and the aggregator. I started consumers `a`, `b`, `c` and `d` one at a time with the producer running, then stopped `b` with Ctrl+C.

**Where the products land:** ETH-USD goes to partition 0. BTC-USD and SOL-USD both hash to partition 2. Partition 1 gets no keys at all, so it stays empty.

**Assignments after each change**, from the `assigned:` lines in each terminal and from `rpk group describe rebalance-demo`:

| After | a | b | c | d |
|---|---|---|---|---|
| `a` starts | 0, 1, 2 | | | |
| `b` joins | 0, 1 | 2 | | |
| `c` joins | 0 | 1 | 2 | |
| `d` joins | 0 | 1 | 2 | none |
| `b` leaves | 0 | | 1 | 2 |

**What the logs showed:**

- **Partitions limit parallelism.** With 4 consumers and 3 partitions, `d` got nothing at first. A consumer beyond the partition count just sits idle.
- **Skewed keys mean skewed work.** At the end, `rpk` showed partition 2 (owned by `d`) with about 4,578 messages and partition 0 (owned by `a`) with about 1,649. Partition 1 (owned by `c`) had none. More consumers didn't spread the work any better, because work is divided by partition, not by consumer.
- **Each rebalance paused every consumer, even ones that kept their partitions.** `a` held partition 0 from the third step onward, yet every time someone joined or left, its log shows `revoked: [0]` followed by `assigned: [0]`. This is the default eager rebalance: every partition is revoked and handed out again.
- **Uninvolved consumers get moved.** When `b` left, `c` moved from partition 2 to partition 1 and `d` picked up partition 2, even though `c` wasn't the one that left. `c`'s log shows `revoked: [2]` then `assigned: [1]`, and `d`'s shows `assigned: []` then `assigned: [2]`. The range assignor (`BALANCER range`) sorts members by ID and assigns partitions in order, so one member leaving shifts the members after it. Moving to partition 1 also left `c` with nothing to read, since that partition is empty.
- **A clean shutdown rebalances right away.** `b` exited through `consumer.close()`, and its partition moved immediately. Compare that with the ghost member in experiment 1, where a crashed consumer kept its spot until the session timeout ran out.

**Takeaways:**

- The partition count is the ceiling on parallelism, and how keys spread across partitions decides how evenly the work divides. Three keys over three partitions is too few for hashing to spread them evenly.
- Eager rebalancing stops the whole group. The `cooperative-sticky` assignor only moves the partitions that actually need to change, so the other consumers keep working through a rebalance.
- This is also why the aggregator only works as a single instance. Its open candles live in memory. If a rebalance moved a partition to another instance, those candles wouldn't move with it, unless `on_revoke` flushed them first.
