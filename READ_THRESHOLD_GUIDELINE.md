# Read Threshold Breach Warn/Block Flow - Implementation Guideline
## Feature: Replica-Side Read Validation with Coordinator Aggregation

### Overview
This implementation provides **replica-side read validation** that tracks tombstones, local read sizes, row index sizes, and SAI index counts. Replicas send warning/abort signals via message parameters, which the coordinator aggregates and processes to warn clients or abort queries. This is a more sophisticated approach than the coordinator-side write validation in PR #2648.

---

## Key Architectural Differences from Write Validation (PR #2648)

| Aspect | Write Validation (PR #2648) | Read Validation (Current) |
|--------|----------------------------|---------------------------|
| **Validation Location** | Coordinator only | Replica-side (data nodes) |
| **Communication** | Direct check before replication | Message parameters in responses |
| **Aggregation** | Not needed (single check) | Coordinator aggregates from multiple replicas |
| **State Management** | Stateless (one-time check) | Stateful (ThreadLocal context per query) |
| **Threshold Types** | Size, Tombstone count | Size, Tombstone, Row Index, SAI Index count |
| **Exception Hierarchy** | Simple (WriteAbortException) | Polymorphic (ReadAbortException + subtypes) |

---

## Architecture Components

### 1. Exception Hierarchy

#### Base Exception: `ReadAbortException.java`
```java
public abstract class ReadAbortException extends ReadFailureException
```
- **Location**: `src/java/org/apache/cassandra/exceptions/ReadAbortException.java`
- **Purpose**: Base class for all read abort exceptions
- **Key Feature**: Abstract class indicating user query violation (not Cassandra failure)

#### Concrete Exceptions:

**1. TombstoneAbortException.java**
- **Fields**: `int nodes`, `long tombstones`
- **Use**: Thrown when tombstone count exceeds failure threshold
- **Message Format**: "{nodes} nodes scanned over {tombstones} tombstones"

**2. ReadSizeAbortException.java**
- **Fields**: Standard ReadAbortException fields only
- **Use**: Thrown for both local read size and row index size violations
- **Reused**: Single exception type for multiple size-based aborts

**3. QueryReferencesTooManyIndexesAbortException.java** (not shown in initial grep)
- **Use**: Thrown when SAI query references too many SSTable indexes

---

### 2. Request Failure Reason System

#### RequestFailureReason.java (Enum)
```java
public enum RequestFailureReason {
    UNKNOWN                                 (0),
    READ_TOO_MANY_TOMBSTONES                (1),
    TIMEOUT                                 (2),
    INCOMPATIBLE_SCHEMA                     (3),
    READ_SIZE                               (4),
    NODE_DOWN                               (5),
    INDEX_NOT_AVAILABLE                     (6),
    READ_TOO_MANY_INDEXES                   (7),
    // ... more
}
```

**Key Features**:
- Integer codes for serialization over network
- Bidirectional mapping: Exception ↔ RequestFailureReason
- Forward compatibility (unknown codes become UNKNOWN)

**Validation**:
- Static initializer ensures all reasons have handlers
- Separate sets: `withoutExceptions` (UNKNOWN, NODE_DOWN, READ_TOO_MANY_INDEXES) vs `withExceptions`

#### RequestFailure.java (Value Object)
```java
public class RequestFailure {
    public final RequestFailureReason reason;
    public final Throwable failure;  // nullable, VERSION_51+
}
```

**Singleton Pattern**: Static instances for common failures
```java
public static final RequestFailure READ_TOO_MANY_TOMBSTONES = ...;
public static final RequestFailure READ_SIZE = ...;
public static final RequestFailure READ_TOO_MANY_INDEXES = ...;
```

---

### 3. Message Parameter System (Replica → Coordinator Communication)

#### ParamType.java (Enum)
```java
public enum ParamType {
    TOMBSTONE_FAIL                   (8,  Int32Serializer.serializer),
    TOMBSTONE_WARNING                (9,  Int32Serializer.serializer),
    LOCAL_READ_SIZE_FAIL             (10, Int64Serializer.serializer),
    LOCAL_READ_SIZE_WARN             (11, Int64Serializer.serializer),
    ROW_INDEX_READ_SIZE_FAIL         (12, Int64Serializer.serializer),
    ROW_INDEX_READ_SIZE_WARN         (13, Int64Serializer.serializer),
    TOO_MANY_REFERENCED_INDEXES_WARN (16, Int32Serializer.serializer),
    TOO_MANY_REFERENCED_INDEXES_FAIL (17, Int32Serializer.serializer),
}
```

**Design**:
- Each threshold type has two ParamTypes: WARN and FAIL
- Integer IDs for serialization (do not reuse old IDs!)
- Type-safe serializers (Int32 for counts, Int64 for bytes)

**Usage Pattern** (on Replica):
```java
// Warning
MessageParams.add(ParamType.LOCAL_READ_SIZE_WARN, sizeInBytes);

// Abort (removes warning first!)
MessageParams.remove(ParamType.LOCAL_READ_SIZE_WARN);
MessageParams.add(ParamType.LOCAL_READ_SIZE_FAIL, sizeInBytes);
```

---

### 4. Replica-Side Threshold Tracking

#### Location 1: ReadCommand.java

**Tombstone Tracking** (lines 677-729):
```java
// In TombstoneCounter class
if (tombstones > failureThreshold) {
    MessageParams.remove(ParamType.TOMBSTONE_WARNING);
    MessageParams.add(ParamType.TOMBSTONE_FAIL, tombstones);
    throw new TombstoneOverwhelmingException(...);
}
if (tombstones > warnThreshold) {
    MessageParams.add(ParamType.TOMBSTONE_WARNING, tombstones);
    logger.warn(msg);
}
```

**Local Read Size Tracking** (lines 799-820):
```java
// In UnfilteredPartitionIterator transformation
if (failBytes != -1 && sizeInBytes >= failBytes) {
    MessageParams.remove(ParamType.LOCAL_READ_SIZE_WARN);
    MessageParams.add(ParamType.LOCAL_READ_SIZE_FAIL, sizeInBytes);
    throw new LocalReadSizeTooLargeException(msg);
}
if (warnBytes != -1 && sizeInBytes >= warnBytes) {
    MessageParams.add(ParamType.LOCAL_READ_SIZE_WARN, sizeInBytes);
}
```

**Key Pattern**:
1. Check abort threshold first
2. If abort: Remove warning param, add fail param, throw exception
3. If warn only: Add warning param, log warning
4. Metrics updated separately (on close)

#### Location 2: RowIndexEntry.java

**Row Index Size Tracking** (lines 394-410):
```java
if (failThreshold != null && estimatedMemory > failThreshold.toBytes()) {
    MessageParams.remove(ParamType.ROW_INDEX_READ_SIZE_WARN);
    MessageParams.add(ParamType.ROW_INDEX_READ_SIZE_FAIL, estimatedMemory);
    throw new RowIndexEntryReadSizeTooLargeException(msg);
}
if (warnThreshold != null && estimatedMemory > warnThreshold.toBytes()) {
    // Use addIfLarger pattern for multiple partitions
    Long current = MessageParams.get(ParamType.ROW_INDEX_READ_SIZE_WARN);
    if (current == null || current.compareTo(estimatedMemory) < 0)
        MessageParams.add(ParamType.ROW_INDEX_READ_SIZE_WARN, estimatedMemory);
}
```

**Special Pattern**: "addIfLarger" for row index warnings
- A query may read multiple partitions with different index sizes
- Only track the maximum value across all partitions

---

### 5. Coordinator-Side Aggregation

#### Data Structures

**WarnAbortCounter.java**:
```java
public class WarnAbortCounter {
    final Set<InetAddressAndPort> warnings;      // Thread-safe set
    final AtomicLong maxWarningValue;            // Highest warning value

    final Set<InetAddressAndPort> aborts;        // Thread-safe set
    final AtomicLong maxAbortsValue;             // Highest abort value

    void addWarning(InetAddressAndPort from, long value);
    void addAbort(InetAddressAndPort from, long value);
    WarningsSnapshot.Warnings snapshot();
}
```

**Design Notes**:
- Concurrent updates from multiple network threads
- Update value first, then add to set (atomic visibility)
- Separate tracking for warnings vs aborts

**WarningContext.java**:
```java
public class WarningContext {
    final WarnAbortCounter tombstones;
    final WarnAbortCounter localReadSize;
    final WarnAbortCounter rowIndexReadSize;
    final WarnAbortCounter indexReadSSTablesCount;

    RequestFailure updateCounters(Map<ParamType, Object> params, InetAddressAndPort from);
    WarningsSnapshot snapshot();
}
```

**updateCounters() Logic**:
1. Iterate through message params
2. For each ParamType, determine counter and failure reason
3. If FAIL param: Add abort, return RequestFailure immediately
4. If WARN param: Add warning, continue processing
5. Return null if no failures

**Supported ParamTypes**:
```java
private static final EnumSet<ParamType> SUPPORTED = EnumSet.of(
    ParamType.TOMBSTONE_WARNING, ParamType.TOMBSTONE_FAIL,
    ParamType.LOCAL_READ_SIZE_WARN, ParamType.LOCAL_READ_SIZE_FAIL,
    ParamType.ROW_INDEX_READ_SIZE_WARN, ParamType.ROW_INDEX_READ_SIZE_FAIL,
    ParamType.TOO_MANY_REFERENCED_INDEXES_WARN, ParamType.TOO_MANY_REFERENCED_INDEXES_FAIL
);
```

**WarningsSnapshot.java**:
```java
public class WarningsSnapshot {
    public final Warnings tombstones;
    public final Warnings localReadSize;
    public final Warnings rowIndexReadSize;
    public final Warnings indexReadSSTablesCount;

    void maybeAbort(ReadCommand command, ConsistencyLevel cl, ...);

    // Nested classes
    static class Warnings {
        public final Counter warnings;
        public final Counter aborts;
    }

    static class Counter {
        public final ImmutableSet<InetAddressAndPort> instances;
        public final long maxValue;
    }
}
```

**Key Methods**:
- `merge()`: Combines snapshots from multiple calls
- `maybeAbort()`: Throws appropriate exception if any abort threshold exceeded
- Message builders: `tombstoneAbortMessage()`, `localReadSizeWarnMessage()`, etc.

#### CoordinatorWarnings.java (ThreadLocal State Management)

**Purpose**: Aggregate warnings across multiple read operations in a single user query

**State Machine**:
```
null → INIT → HashMap<ReadCommand, WarningsSnapshot> → INIT (cleared)
```

**Lifecycle**:
```java
CoordinatorWarnings.init()    // Called at query start
CoordinatorWarnings.update()  // Called when replica responses arrive
CoordinatorWarnings.done()    // Called at query completion
CoordinatorWarnings.reset()   // Called in finally block
```

**ThreadLocal Design**:
```java
private static final FastThreadLocal<Map<ReadCommand, WarningsSnapshot>> STATE;
```
- Uses Netty's FastThreadLocal for performance
- Lazy allocation (only create HashMap when first warning appears)
- Sentinel value INIT (empty map) to detect lifecycle violations

**update() Logic**:
1. Get mutable map (allocate if INIT)
2. Merge new snapshot with existing for same ReadCommand
3. Remove command if merge results in empty snapshot

**done() Logic**:
1. Get readonly snapshot of current state
2. For each ReadCommand with warnings:
   - Get ColumnFamilyStore for metrics
   - Record aborts (metrics + client warnings + logs)
   - Record warnings (metrics + client warnings + logs)
3. Clear state (set to INIT)

**Defensive Checks**:
```java
private static final boolean ENABLE_DEFENSIVE_CHECKS =
    READS_THRESHOLDS_COORDINATOR_DEFENSIVE_CHECKS_ENABLED.getBoolean();
```
- If enabled: Throws AssertionError on lifecycle violations
- If disabled: Gracefully degrades (uses IgnoreMap)

---

### 6. Integration Points

#### ReadCallback.java (Network Response Handler)

**WarningContext Management** (lines 80-82):
```java
private volatile WarningContext warningContext;
private static final AtomicReferenceFieldUpdater<ReadCallback, WarningContext> warningsUpdater
    = AtomicReferenceFieldUpdater.newUpdater(...);
```

**Lazy Initialization** (lines 244-255):
```java
private WarningContext getWarningContext() {
    WarningContext current;
    do {
        current = warningContext;
        if (current != null)
            return current;
        current = new WarningContext();
    } while (!warningsUpdater.compareAndSet(this, null, current));
    return current;
}
```
- Only allocate WarningContext if params actually contain warnings
- Thread-safe lazy initialization via CAS

**onResponse() Processing** (lines 216-230):
```java
Map<ParamType, Object> params = message.header.params();
if (WarningContext.isSupported(params.keySet())) {
    RequestFailure reason = getWarningContext().updateCounters(params, from);
    replicaPlan().collectFailure(message.from(), reason);
    if (reason != null) {
        onFailure(message.from(), reason);  // Treats as failed replica
        return;  // Don't process response further
    }
}
```

**await() Processing** (lines 151-179):
1. Snapshot warnings from WarningContext
2. Update CoordinatorWarnings if snapshot not empty
3. If query failed or timed out: Call `snapshot.maybeAbort()`
   - Throws exception if abort threshold exceeded
   - Includes all replica failure info in exception

**Key Flow**:
```
Replica Response → onResponse() → Update WarningContext
                                 ↓
                    (if FAIL param) → onFailure() → Mark replica as failed
                                 ↓
Query Complete → await() → Snapshot WarningContext
                         ↓
                    Update CoordinatorWarnings
                         ↓
              (if failed) → maybeAbort() → Throw exception
```

---

### 7. Configuration Layer

#### Config.java Fields:
```java
// Tombstone thresholds (legacy, pre-existing)
public volatile int tombstone_warn_threshold = 1000;
public volatile int tombstone_failure_threshold = 100000;

// Local read size thresholds (new style)
public volatile DataStorageSpec.LongBytesBound local_read_size_warn_threshold = null;
public volatile DataStorageSpec.LongBytesBound local_read_size_fail_threshold = null;

// Row index size thresholds
public volatile DataStorageSpec.LongBytesBound row_index_read_size_warn_threshold = null;
public volatile DataStorageSpec.LongBytesBound row_index_read_size_fail_threshold = null;

// Coordinator read size thresholds (separate from replica-side!)
public volatile DataStorageSpec.LongBytesBound coordinator_read_size_warn_threshold = null;
public volatile DataStorageSpec.LongBytesBound coordinator_read_size_fail_threshold = null;

// SAI index count thresholds
public volatile int sai_sstable_indexes_per_query_warn_threshold = 32;
public volatile int sai_sstable_indexes_per_query_fail_threshold = -1;  // disabled
```

#### DatabaseDescriptor Accessors:
```java
// Tombstone (int-based)
public static int getTombstoneWarnThreshold()
public static int getTombstoneFailureThreshold()

// Size-based (LongBytesBound)
public static DataStorageSpec.LongBytesBound getLocalReadSizeWarnThreshold()
public static DataStorageSpec.LongBytesBound getLocalReadSizeFailThreshold()
// ... similar for row_index and coordinator_read_size
```

**DataStorageSpec.LongBytesBound**:
- Supports human-readable sizes: "1KiB", "10MiB", "1GiB"
- Nullable (null = disabled)
- Convert to bytes: `threshold.toBytes()`

---

### 8. Metrics Layer

#### TableMetrics.java:

**Counters** (accumulated totals):
```java
public final Counter tombstoneWarnings;
public final Counter tombstoneFailures;  // Note: "failures" not "aborts"
```

**Meters** (rates):
```java
public final TableMeter localReadSizeWarnings;
public final TableMeter localReadSizeAborts;

public final TableMeter rowIndexSizeWarnings;
public final TableMeter rowIndexSizeAborts;

public final TableMeter tooManySSTableIndexesReadWarnings;
public final TableMeter tooManySSTableIndexesReadAborts;

public final TableMeter coordinatorReadSizeWarnings;
public final TableMeter coordinatorReadSizeAborts;
```

**Histograms** (size distribution):
```java
public final TableHistogram localReadSize;
public final TableHistogram rowIndexSize;
public final TableHistogram coordinatorReadSize;
```

**Metric Naming**:
- Warning metrics: `{MetricType}Warnings`
- Abort metrics: `{MetricType}Aborts` or `{MetricType}Failures`
- Distribution: `{MetricType}` (just the name)

**Update Locations**:
1. **Replica-side** (in ReadCommand, RowIndexEntry):
   - Update on threshold breach (warnings/aborts)
   - Update histogram in `onClose()`
2. **Coordinator-side** (in CoordinatorWarnings.done()):
   - Aggregate warnings from all replicas
   - Update table metrics

---

### 9. SelectStatement Integration (Coordinator Read Size)

**Note**: Coordinator read size is **different** from local read size:
- Local read size: Bytes read from disk on replica
- Coordinator read size: Bytes in result set sent to client

#### SelectStatement.java (lines 1098-1187):

**Warning Check** (after processing):
```java
if (result.shouldWarn(options.getCoordinatorReadSizeWarnThresholdBytes())) {
    String msg = String.format("Read on table %s has exceeded the size warning threshold of %,d bytes",
                               table, options.getCoordinatorReadSizeWarnThresholdBytes());
    ClientWarn.instance.warn(msg + " with " + loggableTokens(options, state));
    logger.warn("{} with query {}", msg, asCQL(options, state));
    store.metric.coordinatorReadSizeWarnings.mark();
}
```

**Abort Check** (during processing, per row):
```java
// After each row is added to result
maybeFail(result, options);

private void maybeFail(ResultSet result, QueryOptions options) {
    if (!options.isReadThresholdsEnabled())
        return;
    if (result.shouldReject(options.getCoordinatorReadSizeAbortThresholdBytes())) {
        String msg = String.format("Read on table %s has exceeded the size failure threshold of %,d bytes",
                                   table, options.getCoordinatorReadSizeAbortThresholdBytes());
        // ... ClientWarn + log + metrics
        throw new CoordinatorReadSizeTooLargeException(msg);
    }
}
```

**Key Design**:
- Check abort threshold **after each row** (incremental checking)
- Allows slightly oversized results (permissive to avoid unreadable rows)
- Can read entire dataset with LIMIT 1 queries even if each row is oversized

---

## File-by-File Change Summary

### Core Exception Files

#### 1. `src/java/org/apache/cassandra/exceptions/ReadAbortException.java`
- **Type**: NEW base class
- **Purpose**: Abstract base for all read abort exceptions
- **Key**: Indicates user query issue, not Cassandra failure
- **Extends**: ReadFailureException

#### 2. `src/java/org/apache/cassandra/exceptions/ReadSizeAbortException.java`
- **Type**: NEW concrete exception
- **Purpose**: Thrown when local read size or row index size exceeds threshold
- **Fields**: Standard ReadAbortException fields (inherits all)
- **Reuse**: Single exception for multiple size-based aborts

#### 3. `src/java/org/apache/cassandra/exceptions/TombstoneAbortException.java`
- **Type**: NEW concrete exception
- **Purpose**: Thrown when tombstone count exceeds threshold
- **Fields**: `int nodes` (replicas involved), `long tombstones` (max count)
- **Special**: Includes metadata about failure for diagnostics

#### 4. `src/java/org/apache/cassandra/exceptions/RequestFailureReason.java`
- **Type**: ENUM (modified)
- **Added Codes**:
  - `READ_TOO_MANY_TOMBSTONES (1)`
  - `READ_SIZE (4)`
  - `READ_TOO_MANY_INDEXES (7)`
- **Purpose**: Serializable failure reasons for network transmission
- **Key**: Bidirectional Exception ↔ Reason mapping

#### 5. `src/java/org/apache/cassandra/exceptions/RequestFailure.java`
- **Type**: VALUE OBJECT (modified)
- **Added Singletons**:
  - `READ_TOO_MANY_TOMBSTONES`
  - `READ_SIZE`
  - `READ_TOO_MANY_INDEXES`
- **Purpose**: Combines reason + optional exception for network transmission
- **Version**: Supports exception details in VERSION_51+

### Coordinator Warning Aggregation

#### 6. `src/java/org/apache/cassandra/service/reads/thresholds/WarnAbortCounter.java`
- **Type**: NEW class
- **Purpose**: Thread-safe counter for warnings/aborts from multiple replicas
- **Fields**:
  - `Set<InetAddressAndPort> warnings/aborts` (concurrent set)
  - `AtomicLong maxWarningValue/maxAbortsValue`
- **Methods**: `addWarning()`, `addAbort()`, `snapshot()`

#### 7. `src/java/org/apache/cassandra/service/reads/thresholds/WarningContext.java`
- **Type**: NEW class
- **Purpose**: Holds 4 WarnAbortCounters (tombstones, localReadSize, rowIndexReadSize, indexReadSSTablesCount)
- **Key Method**: `updateCounters(Map<ParamType, Object> params, InetAddressAndPort from)`
  - Returns `RequestFailure` if any FAIL param found
  - Updates counters for WARN params
  - Returns null if no failures
- **Supported ParamTypes**: EnumSet of 8 types (4 WARN, 4 FAIL)

#### 8. `src/java/org/apache/cassandra/service/reads/thresholds/WarningsSnapshot.java`
- **Type**: NEW class (immutable snapshot)
- **Purpose**: Immutable snapshot of warnings for one or more ReadCommands
- **Structure**:
  - `Warnings` (has `Counter warnings` + `Counter aborts`)
  - `Counter` (has `ImmutableSet<InetAddressAndPort> instances` + `long maxValue`)
- **Key Methods**:
  - `merge()`: Combine snapshots from multiple calls
  - `maybeAbort()`: Throw appropriate exception if abort threshold exceeded
  - Message builders (static): `tombstoneAbortMessage()`, `localReadSizeWarnMessage()`, etc.
- **Empty Pattern**: Uses singleton EMPTY instance

#### 9. `src/java/org/apache/cassandra/service/reads/thresholds/CoordinatorWarnings.java`
- **Type**: NEW class (ThreadLocal state manager)
- **Purpose**: Accumulate warnings across read operations in single query
- **State**: `FastThreadLocal<Map<ReadCommand, WarningsSnapshot>>`
- **Lifecycle Methods**:
  - `init()`: Initialize ThreadLocal to INIT sentinel
  - `update(ReadCommand, WarningsSnapshot)`: Merge new warnings
  - `done()`: Process accumulated warnings, update metrics, send client warnings
  - `reset()`: Clear ThreadLocal state
- **Defensive Mode**: Optional strict checks via system property
- **IgnoreMap Pattern**: Graceful degradation if lifecycle violated

### Network Integration

#### 10. `src/java/org/apache/cassandra/service/reads/ReadCallback.java`
- **Type**: MODIFIED
- **Added Fields**:
  - `volatile WarningContext warningContext`
  - `AtomicReferenceFieldUpdater<ReadCallback, WarningContext> warningsUpdater`
- **Modified Methods**:
  - `onResponse()`: Check for warning params, update context, call onFailure if FAIL param
  - `await()`: Snapshot warnings, update CoordinatorWarnings, maybeAbort if failed
- **Pattern**: Lazy initialization of WarningContext (only if warnings present)

#### 11. `src/java/org/apache/cassandra/net/ParamType.java`
- **Type**: ENUM (modified)
- **Added Entries** (8 total):
  - `TOMBSTONE_FAIL (8, Int32Serializer)`
  - `TOMBSTONE_WARNING (9, Int32Serializer)`
  - `LOCAL_READ_SIZE_FAIL (10, Int64Serializer)`
  - `LOCAL_READ_SIZE_WARN (11, Int64Serializer)`
  - `ROW_INDEX_READ_SIZE_FAIL (12, Int64Serializer)`
  - `ROW_INDEX_READ_SIZE_WARN (13, Int64Serializer)`
  - `TOO_MANY_REFERENCED_INDEXES_WARN (16, Int32Serializer)`
  - `TOO_MANY_REFERENCED_INDEXES_FAIL (17, Int32Serializer)`
- **Design**: Separate WARN/FAIL params for each threshold type

### Replica-Side Tracking

#### 12. `src/java/org/apache/cassandra/db/ReadCommand.java`
- **Type**: MODIFIED
- **Tombstone Tracking** (TombstoneCounter class):
  - Check `tombstone_failure_threshold` → Add TOMBSTONE_FAIL param, throw exception
  - Check `tombstone_warn_threshold` → Add TOMBSTONE_WARNING param, log warning
  - Remove TOMBSTONE_WARNING when TOMBSTONE_FAIL added
- **Local Read Size Tracking** (UnfilteredPartitionIterator transformation):
  - Check `local_read_size_fail_threshold` → Add LOCAL_READ_SIZE_FAIL param, throw exception
  - Check `local_read_size_warn_threshold` → Add LOCAL_READ_SIZE_WARN param
  - Update `cfs.metric.localReadSize` histogram on close
- **Pattern**: Check fail first, then warn; remove warn param if fail

#### 13. `src/java/org/apache/cassandra/io/sstable/format/big/RowIndexEntry.java`
- **Type**: MODIFIED
- **Row Index Size Tracking** (trackThresholds method):
  - Calculate `estimatedMemory` for materialized RowIndexEntry
  - Check `row_index_read_size_fail_threshold` → Add ROW_INDEX_READ_SIZE_FAIL param, throw exception
  - Check `row_index_read_size_warn_threshold` → Add ROW_INDEX_READ_SIZE_WARN param (use "addIfLarger" pattern)
- **Special**: Multiple partitions may be read; track maximum value across all

#### 14. `src/java/org/apache/cassandra/cql3/statements/SelectStatement.java`
- **Type**: MODIFIED
- **Coordinator Read Size Tracking**:
  - Check warn threshold after query completes: `result.shouldWarn()`
  - Check abort threshold incrementally after each row: `result.shouldReject()` in `maybeFail()`
  - Update `cfs.metric.coordinatorReadSize` histogram
- **Design**: Incremental abort checking to avoid unreadable rows

### Configuration

#### 15. `src/java/org/apache/cassandra/config/Config.java`
- **Type**: MODIFIED
- **Added Fields**:
  ```java
  public volatile DataStorageSpec.LongBytesBound local_read_size_warn_threshold = null;
  public volatile DataStorageSpec.LongBytesBound local_read_size_fail_threshold = null;
  public volatile DataStorageSpec.LongBytesBound row_index_read_size_warn_threshold = null;
  public volatile DataStorageSpec.LongBytesBound row_index_read_size_fail_threshold = null;
  public volatile DataStorageSpec.LongBytesBound coordinator_read_size_warn_threshold = null;
  public volatile DataStorageSpec.LongBytesBound coordinator_read_size_fail_threshold = null;
  public volatile int sai_sstable_indexes_per_query_warn_threshold = 32;
  public volatile int sai_sstable_indexes_per_query_fail_threshold = -1;
  ```
- **Note**: Tombstone thresholds were pre-existing (int-based)

#### 16. `src/java/org/apache/cassandra/config/DatabaseDescriptor.java`
- **Type**: MODIFIED
- **Added Accessors**:
  - `getTombstoneWarnThreshold()` / `getTombstoneFailureThreshold()` (pre-existing)
  - `getLocalReadSizeWarnThreshold()` / `getLocalReadSizeFailThreshold()`
  - `getRowIndexSizeWarnThreshold()` / `getRowIndexSizeFailThreshold()`
  - `getCoordinatorReadSizeWarnThreshold()` / `getCoordinatorReadSizeFailThreshold()`
  - SAI index threshold accessors
- **Pattern**: Nullable LongBytesBound for size thresholds (null = disabled)

### Metrics

#### 17. `src/java/org/apache/cassandra/metrics/TableMetrics.java`
- **Type**: MODIFIED
- **Added Counters**:
  - `tombstoneWarnings` (Counter)
  - `tombstoneFailures` (Counter) - pre-existing
- **Added Meters** (rates):
  - `localReadSizeWarnings`, `localReadSizeAborts`
  - `rowIndexSizeWarnings`, `rowIndexSizeAborts`
  - `tooManySSTableIndexesReadWarnings`, `tooManySSTableIndexesReadAborts`
  - `coordinatorReadSizeWarnings`, `coordinatorReadSizeAborts`
- **Added Histograms** (distributions):
  - `localReadSize`, `rowIndexSize`, `coordinatorReadSize`

### Testing

#### 18. `test/distributed/.../thresholds/AbstractClientSizeWarning.java`
- **Type**: NEW abstract test base
- **Purpose**: Common test infrastructure for size warning tests
- **Key Methods**:
  - `totalWarnings()`, `totalAborts()`: Abstract methods to get metric values
  - `assertWarnings()`, `assertAbortWarnings()`: Validate warning messages
  - `noWarnings()`, `warningTest()`, `abortTest()`: Template test methods
- **Setup**: 3-node cluster with native protocol

#### 19. `test/distributed/.../thresholds/LocalReadSizeWarningTest.java`
- **Type**: NEW concrete test
- **Extends**: AbstractClientSizeWarning
- **Configuration**: Sets `local_read_size_warn_threshold = 1KB`, `local_read_size_fail_threshold = 2KB`
- **Assertions**: Checks for "(see local_read_size_warn_threshold)" in warnings
- **Metrics**: Validates LocalReadSize counters and histograms

#### 20. `test/distributed/.../thresholds/RowIndexSizeWarningTest.java`
- **Type**: NEW concrete test (similar to LocalReadSizeWarningTest)
- **Configuration**: Sets `row_index_read_size_warn_threshold`, `row_index_read_size_fail_threshold`

#### 21. `test/distributed/.../thresholds/CoordinatorReadSizeWarningTest.java`
- **Type**: NEW concrete test
- **Configuration**: Sets coordinator thresholds (not replica thresholds)

#### 22. `test/distributed/.../thresholds/TombstoneCountWarningTest.java`
- **Type**: NEW concrete test
- **Configuration**: Sets `tombstone_warn_threshold`, `tombstone_failure_threshold`

#### 23. `test/unit/.../service/reads/thresholds/WarningsSnapshotTest.java`
- **Type**: NEW unit test
- **Purpose**: Test WarningsSnapshot merge logic, message formatting, etc.

---

## Implementation Patterns

### Pattern 1: Replica-Side Threshold Checking

```java
// Template for adding new threshold in replica code
long/int currentValue = ...; // calculate current value

// Get thresholds from config
long/int warnThreshold = DatabaseDescriptor.getXxxWarnThreshold();
long/int failThreshold = DatabaseDescriptor.getXxxFailThreshold();

// Check fail threshold first
if (failThreshold > 0 && currentValue >= failThreshold) {
    // Remove warning param if previously set
    MessageParams.remove(ParamType.XXX_WARN);
    // Add fail param
    MessageParams.add(ParamType.XXX_FAIL, currentValue);
    // Throw exception (this terminates replica processing)
    throw new XxxAbortException(message);
}

// Check warn threshold
if (warnThreshold > 0 && currentValue >= warnThreshold) {
    MessageParams.add(ParamType.XXX_WARN, currentValue);
    // Optional: log warning locally
    logger.warn(message);
}

// Update metrics
if (cfs != null) {
    cfs.metric.xxxHistogram.update(currentValue);
}
```

### Pattern 2: Message Param Handling (Network)

**Add New ParamType** (in ParamType.java):
```java
XXX_WARN (next_id, AppropriateSerializer.serializer),
XXX_FAIL (next_id+1, AppropriateSerializer.serializer),
```

**Important**:
- Use Int32Serializer for counts
- Use Int64Serializer for byte sizes
- Never reuse old IDs
- Always add WARN and FAIL variants

### Pattern 3: Coordinator Aggregation

**Add to WarningContext** (WarningContext.java):
```java
// 1. Add field
final WarnAbortCounter xxxCounter = new WarnAbortCounter();

// 2. Add to SUPPORTED set
private static final EnumSet<ParamType> SUPPORTED = EnumSet.of(
    ...,
    ParamType.XXX_WARN, ParamType.XXX_FAIL
);

// 3. Add case in updateCounters()
case XXX_FAIL:
    reason = RequestFailure.XXX;
case XXX_WARN:
    counter = xxxCounter;
    break;

// 4. Add to snapshot()
return WarningsSnapshot.create(..., xxxCounter.snapshot());
```

**Add to WarningsSnapshot** (WarningsSnapshot.java):
```java
// 1. Add field
public final Warnings xxxMetric;

// 2. Update constructor, create(), merge(), isEmpty()

// 3. Add to maybeAbort()
if (!xxxMetric.aborts.instances.isEmpty())
    throw new XxxAbortException(...);

// 4. Add message builders
public static String xxxAbortMessage(int nodes, long value, String cql) {
    return String.format("...", nodes, value, cql);
}
```

**Add to CoordinatorWarnings** (CoordinatorWarnings.done()):
```java
recordAborts(merged.xxxMetric, cql, loggableTokens,
             cfs.metric.xxxAborts, WarningsSnapshot::xxxAbortMessage);
recordWarnings(merged.xxxMetric, cql, loggableTokens,
               cfs.metric.xxxWarnings, WarningsSnapshot::xxxWarnMessage);
```

### Pattern 4: Exception Hierarchy

**Add New Exception**:
```java
public class XxxAbortException extends ReadAbortException {
    // Add fields if needed for diagnostics
    public final int extraInfo;

    public XxxAbortException(String message, int extraInfo, ...) {
        super(message, consistency, received, blockFor, dataPresent, failureReasonByEndpoint);
        this.extraInfo = extraInfo;
    }
}
```

**Add to RequestFailureReason**:
```java
// 1. Add enum value
XXX_METRIC (next_code),

// 2. Add to exceptionToReasonMap
exceptionToReasonMap.put(XxxAbortException.class, XXX_METRIC);

// 3. Update withoutExceptions if no exception exists
```

**Add to RequestFailure**:
```java
// 1. Add singleton
public static final RequestFailure XXX_METRIC = new RequestFailure(RequestFailureReason.XXX_METRIC);

// 2. Add to forReason() switch
case XXX_METRIC: return XXX_METRIC;

// 3. Optionally add to forException()
if (t instanceof XxxException)
    return XXX_METRIC;
```

### Pattern 5: Metrics Addition

**Add to TableMetrics**:
```java
// 1. Declare fields
public final TableMeter xxxWarnings;
public final TableMeter xxxAborts;
public final TableHistogram xxx;  // if tracking distribution

// 2. Initialize in constructor
xxxWarnings = createTableMeter("XxxWarnings", cfs.keyspace.metric.xxxWarnings);
xxxAborts = createTableMeter("XxxAborts", cfs.keyspace.metric.xxxAborts);
xxx = createTableHistogram("Xxx", cfs.keyspace.metric.xxx, false);

// 3. Release in release()
// (auto-handled by createTable* methods)
```

**Add to KeyspaceMetrics** (if needed):
```java
public final Meter xxxWarnings;
public final Meter xxxAborts;
public final Histogram xxx;

// Initialize in constructor
xxxWarnings = createKeyspaceMeter("XxxWarnings");
...
```

### Pattern 6: Testing Pattern

**Create Test Class**:
```java
public class XxxWarningTest extends AbstractClientSizeWarning {
    @BeforeClass
    public static void setupClass() throws IOException {
        AbstractClientSizeWarning.setupClass();

        // Configure thresholds after cluster start
        CLUSTER.stream().forEach(i -> i.runOnInstance(() -> {
            DatabaseDescriptor.setXxxWarnThreshold(...);
            DatabaseDescriptor.setXxxFailThreshold(...);
        }));
    }

    @Override
    protected void assertWarnings(List<String> warnings) {
        assertThat(warnings).hasSize(1);
        assertThat(warnings.get(0))
            .contains("(see xxx_warn_threshold)")
            .contains("issued ... warnings for query");
    }

    @Override
    protected void assertAbortWarnings(List<String> warnings) {
        assertThat(warnings).hasSize(1);
        assertThat(warnings.get(0))
            .contains("(see xxx_fail_threshold)")
            .contains("aborted the query");
    }

    @Override
    protected long totalWarnings() {
        return CLUSTER.stream()
            .mapToLong(i -> i.metrics().getCounter("org.apache.cassandra.metrics.keyspace.XxxWarnings." + KEYSPACE))
            .sum();
    }

    @Override
    protected long totalAborts() {
        return CLUSTER.stream()
            .mapToLong(i -> i.metrics().getCounter("org.apache.cassandra.metrics.keyspace.XxxAborts." + KEYSPACE))
            .sum();
    }
}
```

---

## Key Design Principles

### 1. Replica-Side Validation
- **Why**: Coordinator doesn't have access to actual data sizes
- **How**: Replicas track metrics during read execution, send via message params
- **Benefit**: Accurate measurement of actual work done

### 2. Aggregation at Coordinator
- **Why**: Multiple replicas may respond with different warnings
- **How**: WarningContext accumulates from all replicas, coordinator aggregates
- **Benefit**: Single warning to client with max value across all replicas

### 3. Separate Warn/Abort Paths
- **Why**: Allow operators to see problems before queries fail
- **How**: Two thresholds (warn < abort), two ParamTypes per metric
- **Benefit**: Gradual degradation, better observability

### 4. ThreadLocal State Management
- **Why**: Single query may trigger multiple read operations
- **How**: CoordinatorWarnings uses ThreadLocal to accumulate across operations
- **Benefit**: Consistent view of all warnings for entire query

### 5. Lazy Initialization
- **Why**: Most queries don't hit thresholds
- **How**: Only allocate WarningContext when params present, only allocate map when first warning
- **Benefit**: Zero overhead for normal queries

### 6. Immutable Snapshots
- **Why**: Concurrent updates from network threads + coordinator thread reading
- **How**: WarnAbortCounter → WarningsSnapshot (immutable)
- **Benefit**: Thread-safe without locking

### 7. Fail-Fast on Replica
- **Why**: Don't waste resources on queries that will fail anyway
- **How**: Throw exception immediately when fail threshold exceeded
- **Benefit**: Saves CPU, disk I/O, network bandwidth

---

## Comparison Table: Write vs Read Thresholds

| Feature | Write (PR #2648) | Read (Current) |
|---------|------------------|----------------|
| **Validation Point** | Coordinator | Replicas |
| **Check Timing** | Before write | During read execution |
| **Data Source** | TopPartitionTracker estimates | Actual measured values |
| **Communication** | Direct check (no network) | Message parameters |
| **State Management** | Stateless | ThreadLocal per query |
| **Aggregation** | Not needed | Coordinator aggregates |
| **Metrics Count** | 2 per threshold (warn/abort) | 3 per threshold (warn/abort/histogram) |
| **Configuration Style** | Custom classes (TrackWarnings) | Standard config fields |
| **Exception Hierarchy** | WriteAbortException → WriteFailureException | ReadAbortException → ReadFailureException |
| **Threshold Types** | 2 (size, tombstone) | 4+ (tombstone, local size, row index, SAI index) |
| **ThreadLocal Usage** | None | CoordinatorWarnings (FastThreadLocal) |
| **Lazy Allocation** | N/A | WarningContext, CoordinatorWarnings map |

---

## Configuration Examples

### cassandra.yaml
```yaml
# Tombstone thresholds (legacy int-based)
tombstone_warn_threshold: 1000
tombstone_failure_threshold: 100000

# Local read size thresholds (new style, human-readable)
local_read_size_warn_threshold: 10MiB
local_read_size_fail_threshold: 100MiB

# Row index size thresholds
row_index_read_size_warn_threshold: 1MiB
row_index_read_size_fail_threshold: 10MiB

# Coordinator read size (result set size, not disk read size)
coordinator_read_size_warn_threshold: 50MiB
coordinator_read_size_fail_threshold: 500MiB

# SAI index count thresholds
sai_sstable_indexes_per_query_warn_threshold: 32
sai_sstable_indexes_per_query_fail_threshold: 64  # or -1 to disable
```

### Runtime Configuration (nodetool/JMX)
```bash
# Get current thresholds
nodetool getlogginglevels  # (not actual command, example)

# Set thresholds dynamically
nodetool settombstonewarnthreshold 2000
nodetool settombstonefailurethreshold 200000
```

---

## Troubleshooting Guide

### Symptom: Warnings not appearing in client

**Check**:
1. Is replica sending params? (check replica logs for warnings)
2. Is WarningContext getting updated? (enable DEBUG logging on ReadCallback)
3. Is CoordinatorWarnings.done() being called? (check coordinator logs)
4. Are thresholds actually exceeded? (check metric histograms)

### Symptom: Queries failing unexpectedly

**Check**:
1. Which threshold is exceeded? (check exception message)
2. Are thresholds too low? (check configuration)
3. Is query actually reading too much? (check table metrics)
4. Is it a specific partition? (check TopPartitionTracker)

### Symptom: Metrics not updating

**Check**:
1. Is TableMetrics created for this table? (check logs on startup)
2. Is ColumnFamilyStore null? (race condition during drop table)
3. Are you checking the right metric name? (keyspace vs table level)
4. Is query actually hitting thresholds? (check logs)

---

## Next Steps for Write Threshold Implementation

Based on this read threshold implementation, to add similar write thresholds:

### Option 1: Replica-Side Write Validation (Recommended)
**Similar to read flow**:
1. Create WriteAbortException hierarchy
2. Add WRITE_SIZE_WARN/FAIL ParamTypes
3. Track write sizes in Mutation processing
4. Send params back to coordinator in write response
5. Aggregate at coordinator in WriteResponseHandler
6. Use CoordinatorWarnings pattern (or create CoordinatorWriteWarnings)

**Advantages**:
- Accurate measurement (actual bytes written)
- Consistent with read threshold design
- Works with all client routing

**Disadvantages**:
- More complex (network round-trip needed)
- Slower (validation after write sent to replicas)

### Option 2: Enhanced Coordinator-Side Validation
**Improve PR #2648 approach**:
1. Keep coordinator-side validation
2. Add actual size calculation (not just estimates)
3. Add configurable enable/disable
4. Improve token-aware routing bug
5. Add more threshold types

**Advantages**:
- Simpler (no network params needed)
- Faster (fail before sending to replicas)
- Already implemented (just needs fixes)

**Disadvantages**:
- Still requires TopPartitionTracker estimates (may be stale)
- Token-aware routing dependency
- Less accurate than replica-side

### Recommendation
Use **Option 1** (replica-side) for consistency with read thresholds and accuracy. The network overhead is acceptable since writes already require acknowledgment from replicas.

---

## Summary

The read threshold implementation is a sophisticated, replica-side validation system with coordinator aggregation. Key architectural patterns include:

1. **Replica-Side Measurement**: Actual metrics tracked during read execution
2. **Message Parameters**: Lightweight communication of warnings/aborts to coordinator
3. **ThreadLocal Aggregation**: CoordinatorWarnings accumulates warnings across multiple read operations
4. **Lazy Initialization**: Zero overhead for queries that don't hit thresholds
5. **Immutable Snapshots**: Thread-safe aggregation without locking
6. **Polymorphic Exceptions**: Type-safe exception hierarchy for different failure types

This design can be adapted for write thresholds by following the same patterns:
- Define ParamTypes for write warnings
- Track write sizes in Mutation processing
- Send params in write responses
- Aggregate in WriteResponseHandler
- Throw WriteAbortException when thresholds exceeded