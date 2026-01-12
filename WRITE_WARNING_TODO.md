# Write Warning Implementation - Step-by-Step TODO
## Replica-Side Write Warnings (No Blocking) Using TopPartitionTracker

---

## 🎯 Implementation Goals

1. ✅ **Warnings Only** - Never block/abort writes (no FAIL threshold)
2. ✅ **Replica-Side Validation** - Check thresholds where data is written
3. ✅ **TopPartitionTracker Lookup** - Use existing estimates like PR #2648
4. ✅ **Message Parameters** - Send warnings to coordinator like read flow
5. ✅ **Coordinator Aggregation** - Aggregate warnings from replicas
6. ✅ **Client Propagation** - Send warnings to client via ClientWarn

---

## 📊 Architecture Overview

```
┌─────────────┐
│   Client    │
│   Request   │
└──────┬──────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────┐
│  COORDINATOR                                                 │
│  1. Receives write request                                   │
│  2. Routes to replicas                                       │
│  3. Waits for responses (via WriteResponseHandler)           │
│  4. Receives message params (WRITE_SIZE_WARN)                │
│  5. Aggregates warnings (CoordinatorWriteWarnings)           │
│  6. Sends warning to client (ClientWarn.instance.warn())     │
└─────────────────────────────────────────────────────────────┘
       │
       ├──────────────┬──────────────┬──────────────┐
       ▼              ▼              ▼              ▼
┌─────────────┐ ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
│  REPLICA 1  │ │  REPLICA 2  │ │  REPLICA 3  │ │  REPLICA N  │
│             │ │             │ │             │ │             │
│ MutationVerbHandler.doVerb() │ │             │ │             │
│ ↓           │ │             │ │             │ │             │
│ Check:      │ │             │ │             │ │             │
│ 1. Get CFS  │ │             │ │             │ │             │
│ 2. Get TopPartitionTracker   │ │             │ │             │
│ 3. Lookup partition estimate │ │             │ │             │
│ 4. Compare with threshold    │ │             │ │             │
│ 5. Add MessageParam if warn  │ │             │ │             │
│ ↓           │ │             │ │             │ │             │
│ Apply mutation (always!)     │ │             │ │             │
│ ↓           │ │             │ │             │ │             │
│ Send response with params    │ │             │ │             │
└─────────────┘ └─────────────┘ └─────────────┘ └─────────────┘
```

---

## 📋 IMPLEMENTATION PHASES

### **PHASE 1: Configuration & Setup** (Foundation)
### **PHASE 2: Network Layer** (Message Parameters)
### **PHASE 3: Replica-Side Measurement** (TopPartitionTracker Lookup)
### **PHASE 4: Coordinator Aggregation** (Warning Collection)
### **PHASE 5: Metrics** (Observability)
### **PHASE 6: Testing** (Validation)

---

## PHASE 1: Configuration & Setup

### ✅ STEP 1.1: Add Configuration Fields
**File**: `src/java/org/apache/cassandra/config/Config.java`

**Action**: Add threshold configuration fields for write warnings

```java
// Add after existing threshold fields (around line 580)

// Write warning thresholds (size-based)
public volatile DataStorageSpec.LongBytesBound write_size_warn_threshold = null;

// Write warning thresholds (tombstone-based)
public volatile int write_tombstone_warn_threshold = 0;
```

**Rationale**:
- Uses `DataStorageSpec.LongBytesBound` for size (consistent with read thresholds)
- Uses `int` for tombstone count (consistent with existing tombstone config)
- Nullable/0 means disabled (consistent with Cassandra config patterns)
- Naming: `write_*` (simple, consistent with read flow)

**Verification**:
- [ ] Config compiles
- [ ] Field naming is consistent with existing patterns
- [ ] Default values are safe (null/0 = disabled)

---

### ✅ STEP 1.2: Add DatabaseDescriptor Accessors
**File**: `src/java/org/apache/cassandra/config/DatabaseDescriptor.java`

**Action**: Add getters/setters for write thresholds

```java
// Add in the appropriate section (search for "getTombstoneWarnThreshold" for reference)

public static DataStorageSpec.LongBytesBound getWriteSizeWarnThreshold()
{
    return conf.write_size_warn_threshold;
}

public static void setWriteSizeWarnThreshold(DataStorageSpec.LongBytesBound threshold)
{
    conf.write_size_warn_threshold = threshold;
}

public static int getWriteTombstoneWarnThreshold()
{
    return conf.write_tombstone_warn_threshold;
}

public static void setWriteTombstoneWarnThreshold(int threshold)
{
    conf.write_tombstone_warn_threshold = Math.max(threshold, 0);
}
```

**Verification**:
- [ ] Getters return correct config values
- [ ] Setters validate input (no negative values for tombstone)
- [ ] Method naming follows existing conventions

---

### ✅ STEP 1.3: Update cassandra.yaml Documentation
**File**: `conf/cassandra.yaml`

**Action**: Add documentation for new thresholds

```yaml
# Add in the "Read/Write Threshold Configuration" section

# Warn when a write exceeds this size threshold
# This is checked against TopPartitionTracker estimates on replicas
# Set to null to disable. Only warnings are issued, writes are never blocked.
# write_size_warn_threshold: 10MiB

# Warn when a write contains more than this many tombstones
# This is checked against TopPartitionTracker estimates on replicas
# Set to 0 to disable. Only warnings are issued, writes are never blocked.
# write_tombstone_warn_threshold: 1000
```

**Verification**:
- [ ] Documentation is clear
- [ ] Examples are reasonable
- [ ] States that writes are never blocked

---

## PHASE 2: Network Layer (Message Parameters)

### ✅ STEP 2.1: Add ParamTypes for Write Warnings
**File**: `src/java/org/apache/cassandra/net/ParamType.java`

**Action**: Add new ParamTypes for write warnings

```java
// Add after the last ParamType (currently TOO_MANY_REFERENCED_INDEXES_FAIL = 17)

WRITE_SIZE_WARN        (18, Int64Serializer.serializer),
WRITE_TOMBSTONE_WARN   (19, Int32Serializer.serializer);
```

**Rationale**:
- Only WARN variants (no FAIL) because we never block writes
- Int64 for size (bytes can be large)
- Int32 for tombstone count (reasonable range)
- Next available IDs (18, 19)

**Verification**:
- [ ] IDs don't conflict with existing ParamTypes
- [ ] Correct serializers chosen
- [ ] Enum compiles without errors

---

## PHASE 3: Replica-Side Measurement

### ✅ STEP 3.1: Add Write Warning Check in Mutation Handler
**File**: `src/java/org/apache/cassandra/db/MutationVerbHandler.java` or `AbstractMutationVerbHandler.java`

**Location**: Find the `doVerb()` method - this is where mutations are processed

**Action**: Add threshold checking logic BEFORE applying the mutation

```java
// Add this method to the class
private void checkWriteWarnings(Mutation mutation)
{
    // Get configuration
    DataStorageSpec.LongBytesBound sizeWarnThreshold = DatabaseDescriptor.getWriteSizeWarnThreshold();
    int tombstoneWarnThreshold = DatabaseDescriptor.getWriteTombstoneWarnThreshold();

    // Quick exit if both disabled
    if (sizeWarnThreshold == null && tombstoneWarnThreshold == 0)
        return;

    long sizeWarnBytes = sizeWarnThreshold != null ? sizeWarnThreshold.toBytes() : -1;

    // Check each partition update in the mutation
    for (PartitionUpdate update : mutation.getPartitionUpdates())
    {
        TableId tableId = update.metadata().id;
        ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(tableId);

        // Skip if table was dropped or no topPartitions tracking
        if (cfs == null || cfs.topPartitions == null)
            continue;

        DecoratedKey key = update.partitionKey();

        // Get estimates from TopPartitionTracker (like PR #2648)
        long estimatedSize = cfs.topPartitions.topSizes().getEstimate(key);
        long estimatedTombstones = cfs.topPartitions.topTombstones().getEstimate(key);

        // Check size threshold
        if (sizeWarnBytes > 0 && estimatedSize > sizeWarnBytes)
        {
            // Use addIfLarger pattern (mutation may have multiple partition updates)
            Long currentSize = MessageParams.get(ParamType.WRITE_SIZE_WARN);
            if (currentSize == null || currentSize < estimatedSize)
            {
                MessageParams.add(ParamType.WRITE_SIZE_WARN, estimatedSize);

                // Log warning on replica
                TableMetadata meta = update.metadata();
                String pk = meta.partitionKeyType.getString(key.getKey());
                logger.warn("Write to {}.{} partition {} triggered size warning; " +
                           "estimated size is {} bytes, threshold is {} bytes (see write_size_warn_threshold)",
                           meta.keyspace, meta.name, pk, estimatedSize, sizeWarnBytes);
            }
        }

        // Check tombstone threshold
        if (tombstoneWarnThreshold > 0 && estimatedTombstones > tombstoneWarnThreshold)
        {
            // Use addIfLarger pattern
            Integer currentTombstones = MessageParams.get(ParamType.WRITE_TOMBSTONE_WARN);
            if (currentTombstones == null || currentTombstones < estimatedTombstones)
            {
                MessageParams.add(ParamType.WRITE_TOMBSTONE_WARN, (int) estimatedTombstones);

                // Log warning on replica
                TableMetadata meta = update.metadata();
                String pk = meta.partitionKeyType.getString(key.getKey());
                logger.warn("Write to {}.{} partition {} triggered tombstone warning; " +
                           "estimated tombstone count is {}, threshold is {} (see write_tombstone_warn_threshold)",
                           meta.keyspace, meta.name, pk, estimatedTombstones, tombstoneWarnThreshold);
            }
        }
    }
}
```

**Integration Point**: Call this in `doVerb()` method

```java
// In doVerb() method, BEFORE applying mutation
public void doVerb(Message<Mutation> message)
{
    // ... existing code ...

    // Check write warnings (new code)
    checkWriteWarnings(mutation);

    // Apply mutation (existing code - always happens!)
    mutation.apply();

    // ... rest of existing code ...
}
```

**Key Design Decisions**:
1. ✅ **Check BEFORE apply** - Get warnings early, but always apply
2. ✅ **Never throw exception** - Warnings only, never block
3. ✅ **Use TopPartitionTracker.getEstimate()** - O(1) lookup from PR #2648
4. ✅ **addIfLarger pattern** - Track max value across multiple partitions in batch
5. ✅ **Log on replica** - Helps debugging which replica detected issue

**IMPORTANT - Batch Mutations Support**:
- ✅ The `for (PartitionUpdate update : mutation.getPartitionUpdates())` loop handles batch mutations automatically
- ✅ When a batch contains multiple partitions (e.g., `BEGIN BATCH INSERT ... INSERT ... APPLY BATCH`), this code checks EACH partition
- ✅ The `addIfLarger` pattern ensures we track the MAXIMUM value across all partitions in the batch
- ✅ A single batch can write to multiple tables - the loop handles this correctly

**Verification**:
- [ ] Method compiles without errors
- [ ] doVerb() integration point identified
- [ ] Mutation.getPartitionUpdates() is correct method
- [ ] MessageParams is available (might need import)
- [ ] Batch mutations are handled by the getPartitionUpdates() loop

---

### ✅ STEP 3.2: Verify MessageParams Availability
**File**: `src/java/org/apache/cassandra/db/MessageParams.java`

**Answer**: MessageParams uses **FastThreadLocal** - Static API, No Parameters Needed!

**How MessageParams Works** (from analyzing ReadCommand.java and MessageParams.java):

1. **Thread-Local Storage**: Uses `FastThreadLocal<Map<ParamType, Object>>` from Netty
2. **Static API**: Just call static methods directly:
   ```java
   MessageParams.add(ParamType.WRITE_SIZE_WARN, value);    // Add param
   MessageParams.get(ParamType.WRITE_SIZE_WARN);           // Get param (for addIfLarger pattern)
   MessageParams.remove(ParamType.WRITE_SIZE_WARN);        // Remove param
   MessageParams.reset();                                  // Clear all params (optional cleanup)
   ```
3. **No Parameters Passed**: It's automatically scoped to current thread - no context to pass!
4. **Lazy Initialization**: Map is created on first access per thread

**Pattern from ReadCommand.java** (lines 682, 720, 805, 810):
```java
// In ReadCommand during execution (replica-side)
if (sizeInBytes >= failThreshold) {
    MessageParams.remove(ParamType.LOCAL_READ_SIZE_WARN);  // Remove warning if upgrading to fail
    MessageParams.add(ParamType.LOCAL_READ_SIZE_FAIL, sizeInBytes);
    throw new LocalReadSizeTooLargeException(msg);
}
else if (sizeInBytes >= warnThreshold) {
    MessageParams.add(ParamType.LOCAL_READ_SIZE_WARN, sizeInBytes);
}
```

**Adding Params to Response** (from ReadCommandVerbHandler.java lines 99, 126):
```java
// In MutationVerbHandler.respond() - MUST DO THIS!
Message<NoPayload> reply = message.emptyResponse();
reply = MessageParams.addToMessage(reply);  // ← This adds thread-local params to the message!
MessagingService.instance().send(reply, respondToAddress);
```

**For Write Side Implementation**:

**Step 1**: In `checkWriteWarnings()` (already in STEP 3.1), just call:
```java
MessageParams.add(ParamType.WRITE_SIZE_WARN, estimatedSize);
```

**Step 2**: In `MutationVerbHandler.respond()`, modify to:
```java
private void respond(Message<?> respondTo, InetAddressAndPort respondToAddress)
{
    Tracing.trace("Enqueuing response to {}", respondToAddress);
    Message<NoPayload> reply = respondTo.emptyResponse();
    reply = MessageParams.addToMessage(reply);  // ← ADD THIS LINE
    MessagingService.instance().send(reply, respondToAddress);
}
```

**Step 3**: (Optional) Add cleanup in finally block:
```java
finally {
    MessageParams.reset();  // Clear thread-local state
}
```

**Verification**:
- [x] MessageParams.add() works in mutation handler context ✅ (static method, works anywhere)
- [x] MessageParams.get() works for checking existing values ✅ (for addIfLarger pattern)
- [x] Params are included in response message ✅ (via MessageParams.addToMessage())
- [x] Thread-local isolation ✅ (each request thread has its own params)
- [x] No imports needed ✅ (MessageParams is in same package: org.apache.cassandra.db)

---

### ✅ STEP 3.3: Add CAS/LWT Write Warning Support
**File**: `src/java/org/apache/cassandra/service/StorageProxy.java` (or similar CAS handling location)

**Location**: Find the `cas()` method - this is where CAS/LWT (Compare-And-Set / Lightweight Transaction) writes are processed

**What are CAS/LWT Writes?**
- **CAS** = Compare-And-Set
- **LWT** = Lightweight Transactions
- Uses Paxos consensus protocol for linearizable writes
- Single partition per operation
- Examples: `INSERT ... IF NOT EXISTS`, `UPDATE ... IF column=value`, `DELETE ... IF condition`

**Action**: Add threshold checking for CAS writes BEFORE Paxos consensus begins

**Pattern from PR #2648** (from analyzing StorageProxy.cas()):
```java
// In StorageProxy.cas() method, BEFORE Paxos consensus
public static RowIterator cas(ClientState state,
                              String keyspaceName,
                              String cfName,
                              DecoratedKey key,
                              CASRequest request,
                              ...)
{
    // ... existing CAS setup code ...

    // Add write warning check (similar to regular mutations)
    Mutation mutation = ...; // Get the mutation being proposed

    // Check write warnings for the CAS mutation
    // CAS is single-partition, so simpler than batch mutations
    DataStorageSpec.LongBytesBound sizeWarnThreshold = DatabaseDescriptor.getWriteSizeWarnThreshold();
    int tombstoneWarnThreshold = DatabaseDescriptor.getWriteTombstoneWarnThreshold();

    if (sizeWarnThreshold != null || tombstoneWarnThreshold > 0)
    {
        // Get CFS for the table
        ColumnFamilyStore cfs = Keyspace.open(keyspaceName).getColumnFamilyStore(cfName);

        if (cfs != null && cfs.topPartitions != null)
        {
            long estimatedSize = cfs.topPartitions.topSizes().getEstimate(key);
            long estimatedTombstones = cfs.topPartitions.topTombstones().getEstimate(key);

            // Check thresholds (same logic as regular writes)
            if (sizeWarnThreshold != null && estimatedSize > sizeWarnThreshold.toBytes())
            {
                MessageParams.add(ParamType.WRITE_SIZE_WARN, estimatedSize);
                logger.warn("CAS write to {}.{} partition {} triggered size warning...", ...);
            }

            if (tombstoneWarnThreshold > 0 && estimatedTombstones > tombstoneWarnThreshold)
            {
                MessageParams.add(ParamType.WRITE_TOMBSTONE_WARN, (int) estimatedTombstones);
                logger.warn("CAS write to {}.{} partition {} triggered tombstone warning...", ...);
            }
        }
    }

    // ... continue with Paxos consensus (existing code) ...
}
```

**Key Differences from Regular Writes**:
- ✅ **Single partition only** - No need for loop over `getPartitionUpdates()`, CAS is always single partition
- ✅ **No addIfLarger pattern needed** - Only one partition to check
- ✅ **Check BEFORE Paxos** - Don't waste consensus rounds on writes that will trigger warnings
- ✅ **Same warning infrastructure** - Uses same MessageParams, same thresholds, same coordinator aggregation

**IMPORTANT - Evidence from PR #2648**:
- PR #2648 DOES check CAS writes in `StorageProxy.cas()` method
- Uses same `trackLargePartitionMutations()` pattern
- Sets `WriteType.CAS` for metrics tracking

**Verification**:
- [ ] Located StorageProxy.cas() method
- [ ] Check added before Paxos consensus begins
- [ ] Single partition handling (no loop needed)
- [ ] Same MessageParams integration as STEP 3.2

---

## PHASE 4: Coordinator Aggregation

### ✅ STEP 4.1: Determine Reuse vs Create New Classes

**Decision Point**: Can we reuse read warning infrastructure?

**Option A: Reuse Existing Classes** (Recommended if compatible)
- Use existing `WarningContext` class
- Use existing `CoordinatorWarnings` ThreadLocal manager
- Add write-specific counter fields

**Option B: Create Separate Write Classes**
- Create `WriteWarningContext`
- Create `CoordinatorWriteWarnings`
- Keep read and write completely separate

**Analysis Needed**:
- [ ] Check if `WarningContext` is read-specific or generic
- [ ] Check if `CoordinatorWarnings` assumes ReadCommand or is generic
- [ ] Decide based on coupling

**Recommendation**: Start with Option B (separate classes) for clarity, can refactor to shared later

---

### ✅ STEP 4.2: Create WriteWarningContext
**File**: `src/java/org/apache/cassandra/service/writes/thresholds/WriteWarningContext.java` (NEW)

**Action**: Create warning counter for write operations

```java
package org.apache.cassandra.service.writes.thresholds;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.locator.InetAddressAndPort;
import org.apache.cassandra.net.ParamType;

/**
 * Accumulates write warning information from replica responses.
 * Similar to WarningContext but for write operations.
 */
public class WriteWarningContext
{
    private static final EnumSet<ParamType> SUPPORTED = EnumSet.of(
        ParamType.WRITE_SIZE_WARN,
        ParamType.WRITE_TOMBSTONE_WARN
    );

    final WarnCounter writeSize = new WarnCounter();
    final WarnCounter writeTombstone = new WarnCounter();

    public static boolean isSupported(Set<ParamType> keys)
    {
        return !Collections.disjoint(keys, SUPPORTED);
    }

    /**
     * Update counters from replica response parameters.
     * Returns null (writes never abort, only warn).
     */
    public void updateCounters(Map<ParamType, Object> params, InetAddressAndPort from)
    {
        for (Map.Entry<ParamType, Object> entry : params.entrySet())
        {
            WarnCounter counter = null;
            switch (entry.getKey())
            {
                case WRITE_SIZE_WARN:
                    counter = writeSize;
                    break;
                case WRITE_TOMBSTONE_WARN:
                    counter = writeTombstone;
                    break;
            }

            if (counter != null)
                counter.addWarning(from, ((Number) entry.getValue()).longValue());
        }
    }

    public WriteWarningsSnapshot snapshot()
    {
        return WriteWarningsSnapshot.create(
            writeSize.snapshot(),
            writeTombstone.snapshot()
        );
    }
}
```

**Verification**:
- [ ] Package structure created: `service/writes/thresholds/`
- [ ] EnumSet contains correct ParamTypes
- [ ] No abort logic (writes never fail)

---

### ✅ STEP 4.3: Create WarnCounter (Simplified)
**File**: `src/java/org/apache/cassandra/service/writes/thresholds/WarnCounter.java` (NEW)

**Action**: Create simple warning-only counter

```java
package org.apache.cassandra.service.writes.thresholds;

import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.cassandra.locator.InetAddressAndPort;

/**
 * Thread-safe counter for write warnings from replicas.
 * Simpler than WarnAbortCounter since writes never abort.
 */
public class WarnCounter
{
    final Set<InetAddressAndPort> warnings = Collections.newSetFromMap(new ConcurrentHashMap<>());
    final AtomicLong maxWarningValue = new AtomicLong();

    void addWarning(InetAddressAndPort from, long value)
    {
        maxWarningValue.accumulateAndGet(value, Math::max);
        // Add last to maintain visibility guarantees
        warnings.add(from);
    }

    public WriteWarningsSnapshot.Counter snapshot()
    {
        return WriteWarningsSnapshot.Counter.create(warnings, maxWarningValue);
    }
}
```

**Verification**:
- [ ] Thread-safe (ConcurrentHashMap + AtomicLong)
- [ ] Simpler than read version (no abort tracking)

---

### ✅ STEP 4.4: Create WriteWarningsSnapshot
**File**: `src/java/org/apache/cassandra/service/writes/thresholds/WriteWarningsSnapshot.java` (NEW)

**Action**: Create immutable snapshot for write warnings

```java
package org.apache.cassandra.service.writes.thresholds;

import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.locator.InetAddressAndPort;

/**
 * Immutable snapshot of write warnings.
 * Simpler than WarningsSnapshot since writes never abort.
 */
public class WriteWarningsSnapshot
{
    private static final WriteWarningsSnapshot EMPTY = new WriteWarningsSnapshot(Counter.EMPTY, Counter.EMPTY);

    public final Counter writeSize;
    public final Counter writeTombstone;

    private WriteWarningsSnapshot(Counter writeSize, Counter writeTombstone)
    {
        this.writeSize = writeSize;
        this.writeTombstone = writeTombstone;
    }

    public static WriteWarningsSnapshot empty()
    {
        return EMPTY;
    }

    public static WriteWarningsSnapshot create(Counter writeSize, Counter writeTombstone)
    {
        if (writeSize == Counter.EMPTY && writeTombstone == Counter.EMPTY)
            return EMPTY;
        return new WriteWarningsSnapshot(writeSize, writeTombstone);
    }

    public boolean isEmpty()
    {
        return this == EMPTY;
    }

    public WriteWarningsSnapshot merge(WriteWarningsSnapshot other)
    {
        if (other == null || other == EMPTY)
            return this;
        return WriteWarningsSnapshot.create(
            writeSize.merge(other.writeSize),
            writeTombstone.merge(other.writeTombstone)
        );
    }

    @VisibleForTesting
    public static String writeSizeWarnMessage(int nodes, long bytes)
    {
        return String.format("%d nodes detected write to large partition; estimated size is %d bytes (see write_size_warn_threshold)",
                           nodes, bytes);
    }

    @VisibleForTesting
    public static String writeTombstoneWarnMessage(int nodes, long tombstones)
    {
        return String.format("%d nodes detected write to partition with many tombstones; estimated count is %d (see write_tombstone_warn_threshold)",
                           nodes, tombstones);
    }

    public static final class Counter
    {
        private static final Counter EMPTY = new Counter(ImmutableSet.of(), 0);

        public final ImmutableSet<InetAddressAndPort> instances;
        public final long maxValue;

        Counter(ImmutableSet<InetAddressAndPort> instances, long maxValue)
        {
            this.instances = instances;
            this.maxValue = maxValue;
        }

        static Counter empty()
        {
            return EMPTY;
        }

        public static Counter create(Set<InetAddressAndPort> instances, AtomicLong maxValue)
        {
            ImmutableSet<InetAddressAndPort> copy = ImmutableSet.copyOf(instances);
            if (copy.isEmpty())
                return EMPTY;
            return new Counter(copy, maxValue.get());
        }

        public Counter merge(Counter other)
        {
            if (other == EMPTY)
                return this;
            ImmutableSet<InetAddressAndPort> copy = ImmutableSet.<InetAddressAndPort>builder()
                                                    .addAll(instances)
                                                    .addAll(other.instances)
                                                    .build();
            return new Counter(copy, Math.max(maxValue, other.maxValue));
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Counter counter = (Counter) o;
            return maxValue == counter.maxValue && Objects.equals(instances, counter.instances);
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(instances, maxValue);
        }
    }
}
```

**Verification**:
- [ ] Immutable (all fields final)
- [ ] Empty singleton pattern
- [ ] Merge logic correct
- [ ] Message formatting methods present

---

### ✅ STEP 4.5: Create CoordinatorWriteWarnings (ThreadLocal Manager)
**File**: `src/java/org/apache/cassandra/service/writes/thresholds/CoordinatorWriteWarnings.java` (NEW)

**Action**: Create ThreadLocal state manager for coordinator

```java
package org.apache.cassandra.service.writes.thresholds;

import java.util.AbstractMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.netty.util.concurrent.FastThreadLocal;
import org.apache.cassandra.db.ColumnFamilyStore;
import org.apache.cassandra.db.DecoratedKey;
import org.apache.cassandra.schema.Schema;
import org.apache.cassandra.schema.TableId;
import org.apache.cassandra.service.ClientWarn;

/**
 * Coordinator-side aggregation of write warnings from replicas.
 * Similar to CoordinatorWarnings but for write operations.
 */
public class CoordinatorWriteWarnings
{
    private static final Logger logger = LoggerFactory.getLogger(CoordinatorWriteWarnings.class);

    // Simple key: TableId + DecoratedKey for the partition being written
    private static final class PartitionKey
    {
        final TableId tableId;
        final DecoratedKey key;

        PartitionKey(TableId tableId, DecoratedKey key)
        {
            this.tableId = tableId;
            this.key = key;
        }

        @Override
        public boolean equals(Object o)
        {
            if (this == o) return true;
            if (!(o instanceof PartitionKey)) return false;
            PartitionKey that = (PartitionKey) o;
            return tableId.equals(that.tableId) && key.equals(that.key);
        }

        @Override
        public int hashCode()
        {
            return 31 * tableId.hashCode() + key.hashCode();
        }
    }

    private static final Map<PartitionKey, WriteWarningsSnapshot> INIT = Collections.emptyMap();
    private static final FastThreadLocal<Map<PartitionKey, WriteWarningsSnapshot>> STATE = new FastThreadLocal<>();

    private CoordinatorWriteWarnings() {}

    public static void init()
    {
        logger.trace("CoordinatorWriteWarnings.init()");
        if (STATE.get() != null)
            return;
        STATE.set(INIT);
    }

    public static void reset()
    {
        logger.trace("CoordinatorWriteWarnings.reset()");
        STATE.remove();
    }

    public static void update(TableId tableId, DecoratedKey key, WriteWarningsSnapshot snapshot)
    {
        logger.trace("CoordinatorWriteWarnings.update({}, {}, {})", tableId, key, snapshot);
        Map<PartitionKey, WriteWarningsSnapshot> map = mutable();
        PartitionKey partitionKey = new PartitionKey(tableId, key);
        WriteWarningsSnapshot previous = map.get(partitionKey);
        WriteWarningsSnapshot updated = previous == null ? snapshot : previous.merge(snapshot);

        if (updated == null || updated.isEmpty())
            map.remove(partitionKey);
        else
            map.put(partitionKey, updated);
    }

    public static void done()
    {
        Map<PartitionKey, WriteWarningsSnapshot> map = readonly();
        logger.trace("CoordinatorWriteWarnings.done() with state {}", map);

        map.forEach((partitionKey, merged) -> {
            ColumnFamilyStore cfs = Schema.instance.getColumnFamilyStoreInstance(partitionKey.tableId);
            if (cfs == null)
                return;

            String pk = cfs.metadata().partitionKeyType.getString(partitionKey.key.getKey());

            // Record write size warnings
            if (!merged.writeSize.instances.isEmpty())
            {
                String msg = String.format("Write to %s.%s partition %s: %s",
                    cfs.metadata().keyspace,
                    cfs.metadata().name,
                    pk,
                    WriteWarningsSnapshot.writeSizeWarnMessage(
                        merged.writeSize.instances.size(),
                        merged.writeSize.maxValue
                    )
                );
                ClientWarn.instance.warn(msg);
                logger.warn(msg);
                cfs.metric.writeSizeWarnings.mark();
            }

            // Record write tombstone warnings
            if (!merged.writeTombstone.instances.isEmpty())
            {
                String msg = String.format("Write to %s.%s partition %s: %s",
                    cfs.metadata().keyspace,
                    cfs.metadata().name,
                    pk,
                    WriteWarningsSnapshot.writeTombstoneWarnMessage(
                        merged.writeTombstone.instances.size(),
                        merged.writeTombstone.maxValue
                    )
                );
                ClientWarn.instance.warn(msg);
                logger.warn(msg);
                cfs.metric.writeTombstoneWarnings.mark();
            }
        });

        clearState();
    }

    private static Map<PartitionKey, WriteWarningsSnapshot> mutable()
    {
        Map<PartitionKey, WriteWarningsSnapshot> map = STATE.get();
        if (map == null)
        {
            // Not initialized, use ignore map
            map = IgnoreMap.get();
        }
        else if (map == INIT)
        {
            map = new HashMap<>();
            STATE.set(map);
        }
        return map;
    }

    private static Map<PartitionKey, WriteWarningsSnapshot> readonly()
    {
        Map<PartitionKey, WriteWarningsSnapshot> map = STATE.get();
        if (map == null)
            map = Collections.emptyMap();
        return map;
    }

    private static void clearState()
    {
        Map<PartitionKey, WriteWarningsSnapshot> map = STATE.get();
        if (map == null || map == INIT)
            return;
        STATE.set(INIT);
    }

    // Ignore map for graceful degradation
    private static final class IgnoreMap extends AbstractMap<Object, Object>
    {
        private static final IgnoreMap INSTANCE = new IgnoreMap();

        private static <K, V> Map<K, V> get()
        {
            return (Map<K, V>) INSTANCE;
        }

        @Override
        public Object put(Object key, Object value)
        {
            return null;
        }

        @Override
        public Set<Entry<Object, Object>> entrySet()
        {
            return Collections.emptySet();
        }
    }
}
```

**Key Design**:
- Uses (TableId + DecoratedKey) as map key since writes can target multiple partitions
- Similar lifecycle to CoordinatorWarnings: init() → update() → done() → reset()

**Verification**:
- [ ] ThreadLocal lifecycle correct
- [ ] PartitionKey equals/hashCode correct
- [ ] ClientWarn integration correct

---

### ✅ STEP 4.6: Integrate with Write Response Handler
**File**: `src/java/org/apache/cassandra/service/AbstractWriteResponseHandler.java`

**Location**: Find the `onResponse()` method

**Action**: Add warning parameter processing

```java
// Add import
import org.apache.cassandra.service.writes.thresholds.CoordinatorWriteWarnings;
import org.apache.cassandra.service.writes.thresholds.WriteWarningContext;
import org.apache.cassandra.service.writes.thresholds.WriteWarningsSnapshot;

// Add field to class
private volatile WriteWarningContext warningContext;
private static final AtomicReferenceFieldUpdater<AbstractWriteResponseHandler, WriteWarningContext> warningsUpdater
    = AtomicReferenceFieldUpdater.newUpdater(AbstractWriteResponseHandler.class, WriteWarningContext.class, "warningContext");

// Add method
private WriteWarningContext getWarningContext()
{
    WriteWarningContext current;
    do {
        current = warningContext;
        if (current != null)
            return current;
        current = new WriteWarningContext();
    } while (!warningsUpdater.compareAndSet(this, null, current));
    return current;
}

// Modify onResponse() method
@Override
public void onResponse(Message<WriteResponse> msg)
{
    // Add this BEFORE existing response processing
    Map<ParamType, Object> params = msg.header.params();
    if (WriteWarningContext.isSupported(params.keySet()))
    {
        getWarningContext().updateCounters(params, msg.from());
    }

    // ... existing response processing code ...
}
```

**Verification**:
- [ ] Find correct response handler class
- [ ] onResponse() method exists
- [ ] Message<WriteResponse> is correct type
- [ ] Import statements needed

---

### ✅ STEP 4.7: Integrate with Write Completion
**Files**:
- `src/java/org/apache/cassandra/service/AbstractWriteResponseHandler.java` (primary)
- `src/java/org/apache/cassandra/transport/Dispatcher.java` (lifecycle management)

**Answer**: Two-Level Integration Pattern (from analyzing read flow)

---

#### **Level 1: Response Handler Integration** (Similar to ReadCallback.await())

**File**: `AbstractWriteResponseHandler.java`

**Location**: In the `get()` method (line 122), AFTER waiting for responses completes

**Action**: Add warning snapshot processing

```java
public void get() throws WriteTimeoutException, WriteFailureException, RetryOnDifferentSystemException
{
    long timeoutNanos = currentTimeoutNanos();

    boolean signaled;
    try
    {
        signaled = condition.await(timeoutNanos, NANOSECONDS);
    }
    catch (InterruptedException e)
    {
        throw new UncheckedInterruptedException(e);
    }

    // ========== ADD WARNING PROCESSING HERE (AFTER WAITING) ==========

    // Get warning context (may be null if no warnings from replicas)
    WriteWarningContext warnings = warningContext;  // ← Use field from STEP 4.6
    WriteWarningsSnapshot snapshot = null;

    if (warnings != null)
    {
        snapshot = warnings.snapshot();

        // Update coordinator warnings if not empty (similar to ReadCallback line 161)
        if (!snapshot.isEmpty())
        {
            // For write, we need to get the mutation to extract partition keys
            // This requires access to the mutation - may need to add as field or parameter
            // Pattern: For each partition in mutation, update coordinator warnings
            // CoordinatorWriteWarnings.update(tableId, partitionKey, snapshot);

            // TODO: Determine how to access mutation here
            // Option 1: Add mutation field to AbstractWriteResponseHandler
            // Option 2: Pass mutation to get() method
            // Option 3: Store partition keys separately during construction
        }
    }

    // ========== END WARNING PROCESSING ==========

    if (!signaled)
        throwTimeout();

    // ... rest of existing get() logic ...
}
```

**Challenge**: Unlike ReadCallback which has access to `ReadCommand`, AbstractWriteResponseHandler needs access to the `Mutation` to get partition keys for `CoordinatorWriteWarnings.update()`.

**Solution Options**:
1. **Add mutation field** to AbstractWriteResponseHandler constructor
2. **Store TableId + DecoratedKey** during construction
3. **Pass mutation** to get() method from caller (StorageProxy)

**Recommended**: Option 2 - Store partition info during construction (cleanest, no API changes)

```java
// Add to AbstractWriteResponseHandler class
private final List<PartitionInfo> partitions = new ArrayList<>();

private static class PartitionInfo {
    final TableId tableId;
    final DecoratedKey key;

    PartitionInfo(TableId tableId, DecoratedKey key) {
        this.tableId = tableId;
        this.key = key;
    }
}

// Add method to populate (call from constructor or before get())
public void addPartition(TableId tableId, DecoratedKey key) {
    partitions.add(new PartitionInfo(tableId, key));
}

// Then in get(), update warnings for all partitions:
if (!snapshot.isEmpty())
{
    for (PartitionInfo partition : partitions)
    {
        CoordinatorWriteWarnings.update(partition.tableId, partition.key, snapshot);
    }
}
```

---

#### **Level 2: Dispatcher Lifecycle Management** (from Dispatcher.java)

**Pattern from Read Flow** (Dispatcher.java lines 383, 426, 459):
```java
// Line 383: Before request execution
if (request.isTrackable())
    CoordinatorWarnings.init();

// Line 423: Execute request
Message.Response response = request.execute(qstate, requestTime);

// Line 426: After request completes successfully
if (request.isTrackable())
    CoordinatorWarnings.done();

// Line 449: In exception handler
if (request.isTrackable())
    CoordinatorWarnings.done();

// Line 459: In finally block (ALWAYS)
CoordinatorWarnings.reset();
```

**For Write Warnings**: Add parallel calls in same locations

**File**: `src/java/org/apache/cassandra/transport/Dispatcher.java`

**Action**: Add CoordinatorWriteWarnings lifecycle calls

```java
static Message.Response processRequest(ServerConnection connection, Message.Request request,
                                      Overload backpressure, RequestTime requestTime)
{
    try
    {
        // ... existing setup ...

        if (request.isTrackable())
        {
            CoordinatorWarnings.init();           // READ warnings (existing)
            CoordinatorWriteWarnings.init();      // WRITE warnings (ADD THIS)
        }

        // ... execute request ...
        Message.Response response = request.execute(qstate, requestTime);

        if (request.isTrackable())
        {
            CoordinatorWarnings.done();           // READ warnings (existing)
            CoordinatorWriteWarnings.done();      // WRITE warnings (ADD THIS)
        }

        return response;
    }
    catch (Throwable t)
    {
        if (request.isTrackable())
        {
            CoordinatorWarnings.done();           // READ warnings (existing)
            CoordinatorWriteWarnings.done();      // WRITE warnings (ADD THIS)
        }
        // ... error handling ...
    }
    finally
    {
        CoordinatorWarnings.reset();              // READ warnings (existing)
        CoordinatorWriteWarnings.reset();         // WRITE warnings (ADD THIS)
        ClientWarn.instance.resetWarnings();
    }
}
```

---

#### **Complete Flow Summary**:

```
1. Dispatcher.processRequest()
   ↓
2. CoordinatorWriteWarnings.init()  ← Dispatcher line ~383
   ↓
3. request.execute() → StorageProxy.mutate()
   ↓
4. AbstractWriteResponseHandler.onResponse()  ← STEP 4.6 (process params from replicas)
   ↓
5. AbstractWriteResponseHandler.get()  ← THIS STEP (snapshot and update)
   ↓
6. CoordinatorWriteWarnings.done()  ← Dispatcher line ~426 (send warnings to client)
   ↓
7. CoordinatorWriteWarnings.reset()  ← Dispatcher line ~459 (cleanup)
```

---

**Verification**:
- [x] Where does write coordinator wait for responses? ✅ AbstractWriteResponseHandler.get() (line 122)
- [x] Where to snapshot warnings? ✅ In get() method after await, before throwing exceptions
- [x] Where to call init/done/reset? ✅ Dispatcher (same location as CoordinatorWarnings)
- [ ] How to access mutation/partition keys? ⚠️ Need to store during construction (see Solution Options above)
- [ ] Integration tested? Will verify in Phase 6

---

**Next Action Items**:
1. Decide on partition info storage approach (Option 1, 2, or 3)
2. Modify AbstractWriteResponseHandler.get() to process warnings
3. Add CoordinatorWriteWarnings.init/done/reset to Dispatcher
4. Test the complete flow end-to-end

---

## PHASE 5: Metrics

### ✅ STEP 5.1: Add Metrics to TableMetrics
**File**: `src/java/org/apache/cassandra/metrics/TableMetrics.java`

**Action**: Add write warning metrics

```java
// Add field declarations (around line 306 with other warning metrics)
public final TableMeter writeSizeWarnings;
public final TableMeter writeTombstoneWarnings;

// Add initialization in constructor (around line 906)
writeSizeWarnings = createTableMeter("WriteSizeWarnings", cfs.keyspace.metric.writeSizeWarnings);
writeTombstoneWarnings = createTableMeter("WriteTombstoneWarnings", cfs.keyspace.metric.writeTombstoneWarnings);

// No explicit release needed (handled by createTableMeter)
```

**Verification**:
- [ ] Fields added in correct section
- [ ] Initialization in constructor
- [ ] Keyspace-level metrics also added (if needed)

---

### ✅ STEP 5.2: Add Keyspace-Level Metrics (if needed)
**File**: `src/java/org/apache/cassandra/metrics/KeyspaceMetrics.java`

**Action**: Add keyspace aggregates

```java
// Add field declarations
public final Meter writeSizeWarnings;
public final Meter writeTombstoneWarnings;

// Add initialization in constructor
writeSizeWarnings = createKeyspaceMeter("WriteSizeWarnings");
writeTombstoneWarnings = createKeyspaceMeter("WriteTombstoneWarnings");
```

**Verification**:
- [ ] Consistent naming with table metrics
- [ ] Proper aggregation from table to keyspace

---

## PHASE 6: Testing

### ✅ STEP 6.1: Create Test Base Class
**File**: `test/distributed/org/apache/cassandra/distributed/test/thresholds/AbstractWriteWarningTest.java` (NEW)

**Action**: Create abstract test base for write warnings

```java
package org.apache.cassandra.distributed.test.thresholds;

import java.io.IOException;
import java.util.List;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.distributed.Cluster;
import org.apache.cassandra.distributed.api.ConsistencyLevel;
import org.apache.cassandra.distributed.api.Feature;
import org.apache.cassandra.distributed.api.ICluster;
import org.apache.cassandra.distributed.api.IInvokableInstance;
import org.apache.cassandra.distributed.test.TestBaseImpl;
import org.apache.cassandra.service.ClientWarn;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class AbstractWriteWarningTest extends TestBaseImpl
{
    protected static ICluster<IInvokableInstance> CLUSTER;

    @BeforeClass
    public static void setupClass() throws IOException
    {
        Cluster.Builder builder = Cluster.build(3);
        builder.withConfig(c -> c.with(Feature.NATIVE_PROTOCOL, Feature.GOSSIP));
        CLUSTER = builder.start();
    }

    @AfterClass
    public static void teardown()
    {
        if (CLUSTER != null)
            CLUSTER.close();
    }

    protected abstract long totalWarnings();
    protected abstract void assertWarnings(List<String> warnings);

    @Before
    public void setup()
    {
        CLUSTER.schemaChange("DROP KEYSPACE IF EXISTS " + KEYSPACE);
        init(CLUSTER);
        CLUSTER.schemaChange("CREATE TABLE " + KEYSPACE + ".tbl (pk int, ck int, v text, PRIMARY KEY (pk, ck))");
    }

    @Test
    public void noWarningsTest()
    {
        // Write to partition that is NOT in top partitions
        CLUSTER.coordinator(1).execute("INSERT INTO " + KEYSPACE + ".tbl (pk, ck, v) VALUES (1, 1, 'test')",
                                       ConsistencyLevel.ALL);

        // No warnings should be generated
        CLUSTER.get(1).runOnInstance(() -> {
            List<String> warnings = ClientWarn.instance.getWarnings();
            assertThat(warnings).isNullOrEmpty();
        });

        assertThat(totalWarnings()).isEqualTo(0);
    }

    @Test
    public void warningTest()
    {
        // This test needs to:
        // 1. Populate TopPartitionTracker with a large partition
        // 2. Configure threshold lower than the partition size
        // 3. Write to that partition
        // 4. Verify warning is generated

        // Implementation depends on how to populate TopPartitionTracker
        // See TombstoneCountWarningTest for reference
    }
}
```

**Verification**:
- [ ] Abstract methods defined
- [ ] Cluster setup correct
- [ ] Test pattern matches read tests

---

### ✅ STEP 6.2: Create Write Size Warning Test
**File**: `test/distributed/org/apache/cassandra/distributed/test/thresholds/WriteSizeWarningTest.java` (NEW)

**Action**: Create concrete test for size warnings

```java
package org.apache.cassandra.distributed.test.thresholds;

import java.io.IOException;
import java.util.List;

import org.junit.BeforeClass;

import org.apache.cassandra.config.DataStorageSpec;
import org.apache.cassandra.config.DatabaseDescriptor;

import static org.apache.cassandra.config.DataStorageSpec.DataStorageUnit.KIBIBYTES;
import static org.assertj.core.api.Assertions.assertThat;

public class WriteSizeWarningTest extends AbstractWriteWarningTest
{
    @BeforeClass
    public static void setupClass() throws IOException
    {
        AbstractWriteWarningTest.setupClass();

        // Set threshold after cluster start
        CLUSTER.stream().forEach(i -> i.runOnInstance(() -> {
            DatabaseDescriptor.setWriteSizeWarnThreshold(new DataStorageSpec.LongBytesBound(1, KIBIBYTES));
        }));
    }

    @Override
    protected void assertWarnings(List<String> warnings)
    {
        assertThat(warnings).hasSize(1);
        assertThat(warnings.get(0))
            .contains("write_size_warn_threshold")
            .contains("estimated size");
    }

    @Override
    protected long totalWarnings()
    {
        return CLUSTER.stream()
            .mapToLong(i -> i.metrics().getCounter("org.apache.cassandra.metrics.keyspace.WriteSizeWarnings." + KEYSPACE))
            .sum();
    }
}
```

**Verification**:
- [ ] Threshold configuration works
- [ ] Warning message format correct
- [ ] Metrics query correct

---

### ✅ STEP 6.3: Create Unit Tests for Aggregation
**File**: `test/unit/org/apache/cassandra/service/writes/thresholds/WriteWarningsSnapshotTest.java` (NEW)

**Action**: Test snapshot merge logic

```java
package org.apache.cassandra.service.writes.thresholds;

import org.junit.Test;

import com.google.common.collect.ImmutableSet;

import org.apache.cassandra.locator.InetAddressAndPort;

import static org.junit.Assert.*;

public class WriteWarningsSnapshotTest
{
    @Test
    public void testEmpty()
    {
        WriteWarningsSnapshot snapshot = WriteWarningsSnapshot.empty();
        assertTrue(snapshot.isEmpty());
        assertEquals(0, snapshot.writeSize.instances.size());
        assertEquals(0, snapshot.writeTombstone.instances.size());
    }

    @Test
    public void testMerge() throws Exception
    {
        InetAddressAndPort addr1 = InetAddressAndPort.getByName("127.0.0.1");
        InetAddressAndPort addr2 = InetAddressAndPort.getByName("127.0.0.2");

        WriteWarningsSnapshot.Counter counter1 = new WriteWarningsSnapshot.Counter(ImmutableSet.of(addr1), 100);
        WriteWarningsSnapshot.Counter counter2 = new WriteWarningsSnapshot.Counter(ImmutableSet.of(addr2), 200);

        WriteWarningsSnapshot snap1 = WriteWarningsSnapshot.create(counter1, WriteWarningsSnapshot.Counter.empty());
        WriteWarningsSnapshot snap2 = WriteWarningsSnapshot.create(counter2, WriteWarningsSnapshot.Counter.empty());

        WriteWarningsSnapshot merged = snap1.merge(snap2);

        assertEquals(2, merged.writeSize.instances.size());
        assertEquals(200, merged.writeSize.maxValue);
    }

    @Test
    public void testMessageFormatting()
    {
        String msg = WriteWarningsSnapshot.writeSizeWarnMessage(3, 1024000);
        assertTrue(msg.contains("3 nodes"));
        assertTrue(msg.contains("1024000 bytes"));
    }
}
```

**Verification**:
- [ ] Tests compile
- [ ] Tests pass
- [ ] Coverage of merge logic

---

## 📊 VERIFICATION CHECKLIST

After all phases complete, verify:

### Compilation
- [ ] All new files compile without errors
- [ ] No missing imports
- [ ] No type mismatches

### Configuration
- [ ] cassandra.yaml has new fields
- [ ] DatabaseDescriptor accessors work
- [ ] Can set thresholds dynamically (JMX/nodetool if implemented)

### Functionality
- [ ] Replica detects large partition writes
- [ ] MessageParams are added to response
- [ ] Coordinator receives params
- [ ] Warnings aggregated correctly
- [ ] ClientWarn.instance.warn() called
- [ ] Client sees warnings

### Performance
- [ ] Zero overhead when thresholds disabled (null/0)
- [ ] O(1) lookup from TopPartitionTracker
- [ ] No memory leaks from ThreadLocal
- [ ] No excessive logging

### Edge Cases
- [ ] Table dropped during write → graceful (null check)
- [ ] Multiple partitions in batch → maxValue tracking works
- [ ] Multiple replicas warn → aggregation correct
- [ ] TopPartitionTracker not initialized → graceful skip
- [ ] CAS/LWT writes trigger warnings correctly
- [ ] Batch mutations check all partitions (verified by getPartitionUpdates() loop)

### Testing
- [ ] Unit tests pass
- [ ] Distributed tests pass
- [ ] Manual testing shows warnings in client
- [ ] Batch mutation tests verify all partitions are checked
- [ ] CAS/LWT write tests verify single-partition CAS warnings work

---

## 🚀 IMPLEMENTATION ORDER

**Recommended sequence**:

1. **Day 1: Configuration** (Phase 1)
   - Add config fields
   - Add DatabaseDescriptor accessors
   - Update cassandra.yaml
   - **Verify**: Code compiles, config accessible

2. **Day 2: Network Layer** (Phase 2)
   - Add ParamTypes
   - **Verify**: Enum compiles

3. **Day 3: Replica Measurement** (Phase 3.1-3.2)
   - Add checkWriteWarnings() method
   - Integrate with MutationVerbHandler
   - Research MessageParams access pattern
   - **Verify**: Code compiles, can add breakpoint

4. **Day 4: Coordinator Aggregation Classes** (Phase 4.2-4.4)
   - Create WriteWarningContext
   - Create WarnCounter
   - Create WriteWarningsSnapshot
   - **Verify**: Classes compile, unit tests work

5. **Day 5: Coordinator Aggregation Integration** (Phase 4.5-4.7)
   - Create CoordinatorWriteWarnings
   - Integrate with WriteResponseHandler
   - Find write completion point
   - **Verify**: Lifecycle works (init → update → done → reset)

6. **Day 6: Metrics** (Phase 5)
   - Add metrics to TableMetrics
   - Add metrics to KeyspaceMetrics
   - **Verify**: Metrics visible via JMX

7. **Day 7: Testing** (Phase 6)
   - Create test base class
   - Create concrete tests
   - Run tests
   - **Verify**: Tests pass

8. **Day 8: Integration Testing & Polish**
   - Manual testing with real cluster
   - Performance testing
   - Edge case testing
   - Documentation

---

## ✅ QUESTIONS ANSWERED

### 1. Batch Mutations - How are they handled?

**Answer: ✅ YES - Check ALL partitions in batch**

**Analysis from existing code**:
- In `StorageProxy.mutateWithTriggers()`: Code iterates `for (IMutation mutation : mutations)` and then `for (TableId tid : mutation.getTableIds())`
- PR #2648 does the same - checks every partition in every mutation in a batch
- Read flow: Each partition in a range query can trigger warnings, aggregated at coordinator

**Example Batch**:
```sql
BEGIN BATCH
  INSERT INTO table1 (pk, v) VALUES (1, 'data');  -- Check partition 1
  INSERT INTO table2 (pk, v) VALUES (2, 'data');  -- Check partition 2
  UPDATE table1 SET v='x' WHERE pk=3;              -- Check partition 3
APPLY BATCH;
```

**Implementation**:
- Iterate through ALL `mutation.getPartitionUpdates()`
- Check each partition against TopPartitionTracker
- Use `MessageParams` addIfLarger pattern to track max value across all partitions
- Coordinator aggregates warnings from all replicas for all partitions

---

### 2. CAS/LWT Writes - What are they and should we check them?

**Answer: ✅ YES - Check CAS writes**

**What is CAS/LWT?**
- **CAS** = Compare-And-Set
- **LWT** = Lightweight Transactions
- Uses Paxos consensus protocol for linearizable writes
- Single partition per operation

**CQL Examples**:
```sql
INSERT INTO users (id, name) VALUES (1, 'Alice') IF NOT EXISTS;
UPDATE users SET email='new@example.com' WHERE id=1 IF email='old@example.com';
DELETE FROM users WHERE id=1 IF status='inactive';
```

**How CAS Works**:
1. Prepare phase (check current values via Paxos)
2. Propose phase (propose new values)
3. Commit phase (actual write if conditions met)
4. Special consistency levels: SERIAL, LOCAL_SERIAL

**Evidence from PR #2648**:
- PR #2648 DOES check CAS writes in `StorageProxy.cas()` method
- Uses same `trackLargePartitionMutations()` pattern
- Sets `WriteType.CAS` for metrics

**Implementation Note**:
- Add check in `StorageProxy.cas()` method (around line 378)
- Check BEFORE Paxos consensus begins
- Single partition operation (simpler than batch)

---

### 3. Metric Naming - Consistency with Read Flow

**Answer: ✅ Remove "partition", use simple "write" prefix**

**Analysis of Read Metrics** (from `TableMetrics.java`):
```java
// Simple names (no "Read" prefix)
public final Counter tombstoneWarnings;
public final Counter tombstoneFailures;

// With location prefix when needed
public final TableMeter localReadSizeWarnings;
public final TableMeter coordinatorReadSizeWarnings;
public final TableMeter rowIndexSizeWarnings;

// NO "partition" prefix anywhere!
```

**Pattern**: `{Location}{Operation}{Metric}{WarningType}`
- `localReadSizeWarnings` = local + Read + Size + Warnings
- `tombstoneWarnings` = (implied) + tombstone + Warnings

**Updated Naming Convention**:

| Old Name (from TODO) | ✅ New Name (Consistent with Read) |
|---------------------|-----------------------------------|
| `partition_write_size_warn_threshold` | `write_size_warn_threshold` |
| `partition_write_tombstone_warn_threshold` | `write_tombstone_warn_threshold` |
| `PARTITION_WRITE_SIZE_WARN` | `WRITE_SIZE_WARN` |
| `PARTITION_WRITE_TOMBSTONE_WARN` | `WRITE_TOMBSTONE_WARN` |
| `partitionWriteSize` | `writeSize` |
| `partitionWriteTombstone` | `writeTombstone` |
| `partitionWriteSizeWarnings` | `writeSizeWarnings` |

**Rationale**:
- Matches existing `tombstoneWarnings` simplicity
- "Write" provides sufficient context
- No "local" needed (write warnings only from replicas)
- Shorter JMX paths for users

---

END OF TODO - Ready for implementation!