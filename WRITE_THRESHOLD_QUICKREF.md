# Read Threshold Implementation - Quick Reference
## File Changes Summary for Write Threshold Implementation

---

## 📋 QUICK FILE CHECKLIST

### Exception Layer (5 files)
- [ ] `exceptions/ReadAbortException.java` - NEW: Abstract base class
- [ ] `exceptions/ReadSizeAbortException.java` - NEW: Size-based abort exception
- [ ] `exceptions/TombstoneAbortException.java` - NEW: Tombstone abort exception
- [ ] `exceptions/RequestFailureReason.java` - MODIFIED: Add READ_SIZE, READ_TOO_MANY_TOMBSTONES codes
- [ ] `exceptions/RequestFailure.java` - MODIFIED: Add singleton instances

### Coordinator Aggregation Layer (4 files)
- [ ] `service/reads/thresholds/WarnAbortCounter.java` - NEW: Thread-safe counter
- [ ] `service/reads/thresholds/WarningContext.java` - NEW: Per-query warning aggregator
- [ ] `service/reads/thresholds/WarningsSnapshot.java` - NEW: Immutable snapshot
- [ ] `service/reads/thresholds/CoordinatorWarnings.java` - NEW: ThreadLocal state manager

### Network Layer (2 files)
- [ ] `service/reads/ReadCallback.java` - MODIFIED: Process warning params from replicas
- [ ] `net/ParamType.java` - MODIFIED: Add 8 new param types (WARN/FAIL pairs)

### Replica Measurement Layer (3 files)
- [ ] `db/ReadCommand.java` - MODIFIED: Track tombstones & local read size
- [ ] `io/sstable/format/big/RowIndexEntry.java` - MODIFIED: Track row index size
- [ ] `cql3/statements/SelectStatement.java` - MODIFIED: Track coordinator read size

### Configuration Layer (2 files)
- [ ] `config/Config.java` - MODIFIED: Add threshold config fields
- [ ] `config/DatabaseDescriptor.java` - MODIFIED: Add getters/setters

### Metrics Layer (1 file)
- [ ] `metrics/TableMetrics.java` - MODIFIED: Add meters, counters, histograms

### Testing Layer (5+ files)
- [ ] `test/distributed/thresholds/AbstractClientSizeWarning.java` - NEW
- [ ] `test/distributed/thresholds/LocalReadSizeWarningTest.java` - NEW
- [ ] `test/distributed/thresholds/RowIndexSizeWarningTest.java` - NEW
- [ ] `test/distributed/thresholds/CoordinatorReadSizeWarningTest.java` - NEW
- [ ] `test/distributed/thresholds/TombstoneCountWarningTest.java` - NEW

**TOTAL: ~23 files** (8 new classes, 15 modifications)

---

## 🔄 ADAPTATION FOR WRITE THRESHOLDS

### Files You'll Need to Create/Modify

#### NEW Exception Files (Write Equivalents)
```
exceptions/WriteAbortException.java          ← Like ReadAbortException
exceptions/WriteSizeAbortException.java      ← Like ReadSizeAbortException
exceptions/WriteTombstoneAbortException.java ← Like TombstoneAbortException (maybe)
```

#### NEW Coordinator Layer (Reuse or Create)
```
Option A: Reuse CoordinatorWarnings (rename to generic ThresholdWarnings)
Option B: Create separate:
  service/writes/thresholds/WriteWarningContext.java
  service/writes/thresholds/CoordinatorWriteWarnings.java
```

#### MODIFY Network Layer
```
net/ParamType.java                           ← Add WRITE_SIZE_WARN/FAIL
service/AbstractWriteResponseHandler.java    ← Process warning params
```

#### MODIFY Replica Measurement
```
db/Mutation.java or MutationVerbHandler       ← Track write sizes
db/ColumnFamilyStore.java                    ← Track on apply
```

#### MODIFY Configuration
```
config/Config.java                           ← Add write threshold fields
config/DatabaseDescriptor.java               ← Add accessors
```

#### MODIFY Metrics
```
metrics/TableMetrics.java                    ← Add write warning/abort meters
```

#### NEW Tests
```
test/distributed/thresholds/WriteWarningTest.java
test/distributed/thresholds/WriteTombstoneWarningTest.java
```

---

## 📊 CHANGE MATRIX BY LAYER

| Layer | Read Implementation | Write Equivalent |
|-------|-------------------|------------------|
| **Exception** | ReadAbortException + 2 subtypes | WriteAbortException + subtypes |
| **Failure Reason** | READ_SIZE, READ_TOO_MANY_TOMBSTONES | WRITE_SIZE, WRITE_TOO_MANY_TOMBSTONES |
| **ParamType** | 8 types (4 metrics × 2) | 4-8 types (depends on metrics) |
| **Context** | WarningContext (4 counters) | WriteWarningContext |
| **Aggregator** | CoordinatorWarnings (ThreadLocal) | CoordinatorWriteWarnings or reuse |
| **Measurement** | ReadCommand, RowIndexEntry | Mutation, ColumnFamilyStore |
| **Response Handler** | ReadCallback.onResponse() | AbstractWriteResponseHandler.onResponse() |
| **Config** | local_read_size_*, row_index_* | write_size_*, write_tombstone_* |
| **Metrics** | LocalReadSize*, RowIndexSize* | WriteSize*, WriteTombstone* |

---

## 🎯 KEY IMPLEMENTATION DECISIONS FOR WRITES

### Decision 1: Where to Validate?

**Option A: Replica-Side (Recommended)**
- ✅ Accurate measurement (actual bytes written)
- ✅ Consistent with read threshold architecture
- ✅ Works with all routing strategies
- ❌ Requires network round-trip
- ❌ More complex implementation

**Option B: Coordinator-Side (PR #2648)**
- ✅ Simpler, already implemented
- ✅ Fail-fast (before sending to replicas)
- ❌ Uses estimates (TopPartitionTracker)
- ❌ Token-aware routing bug
- ❌ Estimates may be stale

### Decision 2: When to Validate?

**Replica-Side Options**:
1. **During Mutation Application** (in ColumnFamilyStore.apply)
   - Pro: Most accurate
   - Con: Too late (already written to commitlog)

2. **In MutationVerbHandler** (before apply)
   - Pro: Can abort before writing
   - Con: Need to calculate size upfront

3. **After CommitLog, Before Memtable**
   - Pro: Balance accuracy vs performance
   - Con: Commitlog already written

**Recommended**: Option 2 (MutationVerbHandler) for replica-side OR coordinator-side for fail-fast

### Decision 3: What to Track?

**Write-Specific Metrics**:
- [x] **Write size** (bytes in mutation)
- [x] **Write tombstone count** (deletions in mutation)
- [ ] **Write row count** (optional)
- [ ] **Write cell count** (optional)
- [ ] **Large cell size** (optional, individual cell > threshold)

**Minimum Viable**: Write size + tombstone count (matches read implementation)

---

## 🛠️ STEP-BY-STEP IMPLEMENTATION GUIDE

### Phase 1: Foundation (Exceptions & Config)
1. Create `WriteAbortException` base class
2. Create `WriteSizeAbortException`
3. Add `WRITE_SIZE` to RequestFailureReason
4. Add `WRITE_SIZE` to RequestFailure
5. Add config fields to Config.java
6. Add accessors to DatabaseDescriptor.java

### Phase 2: Network Layer (ParamTypes)
7. Add `WRITE_SIZE_WARN` (ParamType)
8. Add `WRITE_SIZE_FAIL` (ParamType)
9. (Optional) Add tombstone variants

### Phase 3: Coordinator Aggregation
10. Create `WriteWarningContext` (or reuse WarningContext)
11. Create `CoordinatorWriteWarnings` (or reuse CoordinatorWarnings)
12. Modify `AbstractWriteResponseHandler.onResponse()` to process params

### Phase 4: Replica Measurement
13. Add size calculation in MutationVerbHandler or Mutation
14. Add threshold checks
15. Add MessageParams.add() calls
16. Throw exception if fail threshold exceeded

### Phase 5: Metrics
17. Add metrics to TableMetrics
18. Update metrics in CoordinatorWriteWarnings.done()
19. Update histogram on replica side

### Phase 6: Testing
20. Create AbstractWriteSizeWarning test base
21. Create WriteSizeWarningTest
22. Create WriteTombstoneWarningTest (if applicable)
23. Create unit tests for aggregation logic

### Phase 7: Documentation
24. Update cassandra.yaml with new configs
25. Update NEWS.txt
26. Update CHANGES.txt

---

## 📝 CODE TEMPLATES

### Template 1: Exception Class
```java
package org.apache.cassandra.exceptions;

import java.util.Map;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.locator.InetAddressAndPort;

public class WriteSizeAbortException extends WriteAbortException {
    public WriteSizeAbortException(String msg, ConsistencyLevel consistency,
                                   int received, int blockFor, WriteType writeType,
                                   Map<InetAddressAndPort, RequestFailureReason> failureReasonByEndpoint) {
        super(msg, consistency, received, blockFor, writeType, failureReasonByEndpoint);
    }
}
```

### Template 2: ParamType Addition
```java
// In ParamType.java enum
WRITE_SIZE_WARN (18, Int64Serializer.serializer),
WRITE_SIZE_FAIL (19, Int64Serializer.serializer),
```

### Template 3: Replica-Side Measurement
```java
// In MutationVerbHandler or similar
long sizeInBytes = mutation.serializedSize(MessagingService.current_version);

DataStorageSpec.LongBytesBound warnThreshold = DatabaseDescriptor.getWriteSizeWarnThreshold();
DataStorageSpec.LongBytesBound failThreshold = DatabaseDescriptor.getWriteSizeFailThreshold();

long warnBytes = warnThreshold != null ? warnThreshold.toBytes() : -1;
long failBytes = failThreshold != null ? failThreshold.toBytes() : -1;

if (failBytes != -1 && sizeInBytes >= failBytes) {
    MessageParams.remove(ParamType.WRITE_SIZE_WARN);
    MessageParams.add(ParamType.WRITE_SIZE_FAIL, sizeInBytes);
    String msg = String.format("Write size %d bytes exceeds fail threshold %s",
                               sizeInBytes, failThreshold);
    throw new WriteSizeTooLargeException(msg);
}

if (warnBytes != -1 && sizeInBytes >= warnBytes) {
    MessageParams.add(ParamType.WRITE_SIZE_WARN, sizeInBytes);
}
```

### Template 4: Coordinator Aggregation
```java
// In AbstractWriteResponseHandler.onResponse()
Map<ParamType, Object> params = message.header.params();
if (WriteWarningContext.isSupported(params.keySet())) {
    RequestFailure reason = getWarningContext().updateCounters(params, from);
    if (reason != null) {
        onFailure(message.from(), reason);
        return;
    }
}

// In get() or await() method
WriteWarningContext warnings = warningContext;
if (warnings != null) {
    WarningsSnapshot snapshot = warnings.snapshot();
    if (!snapshot.isEmpty()) {
        CoordinatorWriteWarnings.update(mutation, snapshot);
    }

    // If write failed, maybe abort
    if (failed) {
        snapshot.maybeAbort(mutation, consistencyLevel, received, blockFor, failureReasons);
    }
}
```

### Template 5: Config Fields
```java
// In Config.java
public volatile DataStorageSpec.LongBytesBound write_size_warn_threshold = null;
public volatile DataStorageSpec.LongBytesBound write_size_fail_threshold = null;

// In DatabaseDescriptor.java
public static DataStorageSpec.LongBytesBound getWriteSizeWarnThreshold() {
    return conf.write_size_warn_threshold;
}

public static void setWriteSizeWarnThreshold(DataStorageSpec.LongBytesBound threshold) {
    conf.write_size_warn_threshold = threshold;
}
```

### Template 6: Metrics
```java
// In TableMetrics.java

// Declaration
public final TableMeter writeSizeWarnings;
public final TableMeter writeSizeAborts;
public final TableHistogram writeSize;

// Initialization (in constructor)
writeSizeWarnings = createTableMeter("WriteSizeWarnings", cfs.keyspace.metric.writeSizeWarnings);
writeSizeAborts = createTableMeter("WriteSizeAborts", cfs.keyspace.metric.writeSizeAborts);
writeSize = createTableHistogram("WriteSize", cfs.keyspace.metric.writeSize, false);

// No explicit release needed (auto-handled)
```

---

## 🔍 VALIDATION CHECKLIST

Before considering implementation complete:

### Functionality
- [ ] Warnings appear in client output
- [ ] Aborts throw correct exception type
- [ ] Metrics are updated correctly
- [ ] Configuration changes take effect dynamically
- [ ] Thresholds can be disabled (set to null or -1)

### Edge Cases
- [ ] Multiple replicas with different warnings (aggregation works)
- [ ] Write to multiple tables in batch (each tracked separately)
- [ ] Concurrent writes (ThreadLocal isolation)
- [ ] Table drop during write (graceful handling)
- [ ] Consistency level < ALL (partial failures handled)

### Performance
- [ ] Zero overhead when thresholds disabled
- [ ] Minimal overhead when enabled but not triggered
- [ ] No memory leaks from ThreadLocal state
- [ ] No excessive object allocation

### Testing
- [ ] Unit tests for aggregation logic
- [ ] Distributed tests for end-to-end flow
- [ ] Tests for each threshold type
- [ ] Tests for warning + abort combinations
- [ ] Tests for metric validation

### Documentation
- [ ] cassandra.yaml documented
- [ ] NEWS.txt updated
- [ ] CHANGES.txt updated
- [ ] Logging levels documented

---

## 💡 KEY INSIGHTS FROM READ IMPLEMENTATION

### Insight 1: Lazy Allocation is Critical
- Don't create WarningContext unless warnings actually occur
- Don't create HashMap in CoordinatorWarnings until first warning
- Use sentinel values (INIT, EMPTY) to detect state

### Insight 2: Thread Safety Without Locking
- WarnAbortCounter uses concurrent collections + atomics
- WarningsSnapshot is immutable (snapshot pattern)
- FastThreadLocal for per-thread state

### Insight 3: Remove-Then-Add Pattern
```java
// When upgrading from warning to abort
MessageParams.remove(ParamType.XXX_WARN);
MessageParams.add(ParamType.XXX_FAIL, value);
```
This ensures only one param is sent (either WARN or FAIL, not both)

### Insight 4: MaxValue Tracking
- Track both: Set<InetAddressAndPort> + maxValue
- Set: Which replicas warned/aborted
- maxValue: Highest value across all replicas
- Use atomic updates: `maxValue.accumulateAndGet(value, Math::max)`

### Insight 5: Defensive Programming
- Check for null ColumnFamilyStore (table may be dropped)
- Empty check before processing snapshots
- Optional defensive mode for development

---

## 🚀 RECOMMENDED IMPLEMENTATION ORDER

### For Write Thresholds (Replica-Side Approach)

**Week 1: Foundation**
1. Create exception hierarchy
2. Add RequestFailureReason codes
3. Add configuration fields
4. Add ParamTypes

**Week 2: Replica Measurement**
5. Add size calculation logic
6. Add threshold checks
7. Add MessageParams.add() calls
8. Add metrics updates

**Week 3: Coordinator Aggregation**
9. Create WriteWarningContext (or reuse)
10. Create CoordinatorWriteWarnings (or reuse)
11. Integrate with WriteResponseHandler
12. Add metrics to TableMetrics

**Week 4: Testing & Polish**
13. Write unit tests
14. Write distributed tests
15. Performance testing
16. Documentation

---

## 📞 QUICK COMPARISON REFERENCE

| Feature | Read (Current) | Write (To Implement) |
|---------|---------------|---------------------|
| **Validation Location** | Replica (ReadCommand) | Replica (MutationVerbHandler) OR Coordinator |
| **ParamTypes** | 8 (4 metrics × 2) | 4+ (2 metrics × 2 minimum) |
| **Exception Base** | ReadAbortException | WriteAbortException |
| **Context Class** | WarningContext | WriteWarningContext (or reuse) |
| **Aggregator** | CoordinatorWarnings | CoordinatorWriteWarnings (or reuse) |
| **Response Handler** | ReadCallback | AbstractWriteResponseHandler |
| **Metrics** | 12+ (4 metrics × 3 each) | 6+ (2 metrics × 3 each) |
| **Config Style** | DataStorageSpec.LongBytesBound | Same |
| **Test Base** | AbstractClientSizeWarning | AbstractWriteSizeWarning (new) |

---

## END OF QUICK REFERENCE

See `READ_THRESHOLD_GUIDELINE.md` for detailed implementation patterns and architecture.