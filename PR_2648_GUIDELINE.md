# PR #2648 Implementation Guideline
## Feature: Block Writes to Keys Over Threshold (Coordinator-Side)

### Overview
This PR implements coordinator-side validation to warn or block writes when partitions exceed configurable size or tombstone thresholds. The implementation uses existing TopPartitionTracker estimates to make fast decisions without reading the full partition.

---

## Architecture Components

### 1. Configuration Layer

#### New Class: `TrackWarnings.java`
- **Location**: `src/java/org/apache/cassandra/config/TrackWarnings.java`
- **Purpose**: Configuration holder for write tracking thresholds
- **Key Fields**:
  - `enabled` (boolean): Master toggle for all tracking
  - `partition_write_size` (LongByteThreshold): Size thresholds in KB
  - `partition_write_tombstone` (IntThreshold): Tombstone count thresholds

#### Inner Classes:
1. **LongByteThreshold**: For size-based thresholds
   - `warn_threshold_kb`: Size that triggers warning
   - `abort_threshold_kb`: Size that blocks the write

2. **IntThreshold**: For count-based thresholds
   - `warn_threshold`: Count that triggers warning
   - `abort_threshold`: Count that blocks the write

#### Validation Rules:
- Negative values are clamped to 0
- Abort threshold must be ≥ warn threshold (or 0 to disable)
- Validation happens in `validate()` method with prefix for error messages

#### DatabaseDescriptor Integration:
Added getters/setters in `DatabaseDescriptor.java`:
- `getTrackWarningsEnabled()` / `setTrackWarningsEnabled(boolean)`
- `getPartitionWriteSizeWarnThresholdKb()` / `setPartitionWriteSizeWarnThresholdKb(long)`
- `getPartitionWriteSizeAbortThresholdKb()` / `setPartitionWriteSizeAbortThresholdKb(long)`
- `getPartitionWriteTombstoneWarnThreshold()` / `setPartitionWriteTombstoneWarnThreshold(int)`
- `getPartitionWriteTombstoneAbortThreshold()` / `setPartitionWriteTombstoneAbortThreshold(int)`

---

### 2. Exception Handling

#### New Exception: `WriteAbortException.java`
- **Extends**: `WriteFailureException`
- **Purpose**: Thrown when write exceeds abort threshold
- **Constructor**: Takes message, consistency level, and write type
- **Reason Code**: Uses `RequestFailureReason.WRITE_TOO_LARGE`

#### Modified: `WriteFailureException.java`
- Added constructors to support custom messages with single endpoint
- Used when creating abort exceptions with local coordinator address

#### New Failure Reason: `RequestFailureReason.WRITE_TOO_LARGE`
- **Value**: 5
- **Usage**: Indicates write was blocked due to size/tombstone threshold

---

### 3. TopPartitionTracker Enhancement

#### Problem Solved:
Original TopPartitionTracker could track top partitions but required O(n) search to find a specific partition's estimate.

#### Solution:
Added `ObjectLongMap<DecoratedKey> lookup` to `TopHolder` class.

#### Key Changes in `TopPartitionTracker.java`:

1. **New Field in TopHolder**:
   ```java
   public final ObjectLongMap<DecoratedKey> lookup;
   ```

2. **Updated track() Method**:
   - When adding partition: `lookup.put(tp.key, tp.value)`
   - When evicting partition: `lookup.remove(p.key)`

3. **New Method**:
   ```java
   public long getEstimate(DecoratedKey dk) {
       return lookup.getOrDefault(dk, 0L);
   }
   ```

4. **Constructor Updates**:
   - Initialize lookup map alongside TreeSet
   - Clone lookup map when merging holders

#### Benefits:
- O(1) lookup time for partition estimates
- Maintains existing top-N tracking functionality
- Minimal memory overhead (only stores DecoratedKey -> long mapping)

---

### 4. StorageProxy Write Path Integration

#### Core Method: `trackLargePartitionMutations()`
**Signature**:
```java
private static void trackLargePartitionMutations(
    ConsistencyLevel consistency,
    WriteType writeType,
    TableId cfId,
    DecoratedKey key,
    long sizeBytesWarn,
    long sizeBytesAbort,
    int tombstoneWarn,
    int tombstoneAbort)
```

**Logic Flow**:
1. Get ColumnFamilyStore for the table
2. Check if topPartitions tracker exists
3. Get estimates from tracker:
   - `estimateBytes = topPartitions.topSizes().getEstimate(key)`
   - `estimateTombstone = topPartitions.topTombstones().getEstimate(key)`
4. Check size thresholds (abort first, then warn)
5. Check tombstone thresholds (abort first, then warn)
6. For each violation:
   - Log warning via `logger.warn()`
   - Send client warning via `ClientWarn.instance.warn()`
7. If any abort threshold exceeded: throw `WriteAbortException`

#### Warning Message Format:
```
"Mutation to {table} against partition {pk} triggered {abortion|warning};
partition estimated {size|tombstone} is {estimate}, but threshold was {threshold}"
```

#### Integration Points:

1. **CAS Operations** (`cas()` method):
   ```java
   if (DatabaseDescriptor.getTrackWarningsEnabled()) {
       long sizeBytesWarn = DatabaseDescriptor.getPartitionWriteSizeWarnThresholdKb() * 1024;
       long sizeBytesAbort = DatabaseDescriptor.getPartitionWriteSizeAbortThresholdKb() * 1024;
       int tombstoneWarn = DatabaseDescriptor.getPartitionWriteTombstoneWarnThreshold();
       int tombstoneAbort = DatabaseDescriptor.getPartitionWriteTombstoneAbortThreshold();
       trackLargePartitionMutations(consistencyForPaxos, WriteType.CAS,
                                    metadata.id, key,
                                    sizeBytesWarn, sizeBytesAbort,
                                    tombstoneWarn, tombstoneAbort);
   }
   ```
   **TODO/Bug**: Comment indicates "this is a bug, it works under token aware routing... need to push this to the replica side like local read..."

2. **Regular Mutations** (`mutateWithTriggers()` method):
   - Added before trigger execution
   - Checks if either size or tombstone tracking is enabled
   - Iterates over all mutations and their table IDs
   - Calls `trackLargePartitionMutations()` for each partition
   - **TODO**: Comment asks "how do we know the write type?" (uses WriteType.SIMPLE)

---

## Testing Strategy

### 1. Distributed Test: `HiddenTableTest.java`
- **Purpose**: Tests system table access control (for top_partitions table)
- **Setup**: 3-node cluster with authentication enabled
- **Test Case**: Verifies only superusers can access `system.top_partitions`
- **Pattern**: Shows how to set up auth in distributed tests

### 2. Main Test: `PartitionWriteSizeWarningTest.java`
- **Location**: Referenced but file content not shown in diff
- **Expected Coverage**:
  - Warning threshold triggers
  - Abort threshold triggers
  - Size-based blocking
  - Tombstone-based blocking
  - Client warning delivery

### 3. Test Utilities Added:

#### `JavaDriverUtils.java` Enhancement:
- Added `AuthProvider` support to driver creation
- New overloads:
  - `create(ICluster, AuthProvider)`
  - `create(ICluster, ProtocolVersion, AuthProvider)`

#### `AssertUtil.java` New Helpers:
- `assertAll()` methods for bulk assertions with multiple parameters
- Collects all assertion failures before throwing
- Useful for testing multiple threshold combinations

---

## Implementation Details

### Threshold Checking Logic:
```
if (abort > 0 && estimate > abort) {
    // Log + ClientWarn + collect abort message
} else if (warn > 0 && estimate > warn) {
    // Log + ClientWarn only
}
```

### Configuration Flexibility:
- Set both thresholds to 0: Feature disabled for that metric
- Set warn only: Warning without blocking
- Set abort only: No warning, direct blocking at threshold
- Set both: Warn at lower value, block at higher value

### Performance Considerations:
1. **Fast Path**: If tracking disabled, no overhead
2. **O(1) Lookup**: Uses hash map for partition estimate lookup
3. **Estimates Only**: No actual partition read required
4. **Coordinator-Side**: Check happens before replication

### Known Limitations (from TODO comments):
1. **Token-Aware Routing Bug**: Doesn't work correctly for non-token-aware routing
   - Need to push check to replica side (like local read)
2. **Write Type Detection**: Can't always determine correct WriteType for mutations
3. **Estimate Accuracy**: Relies on TopPartitionTracker estimates which may be stale

---

## Configuration Example (cassandra.yaml):
```yaml
track_warnings:
    enabled: true
    partition_write_size:
        warn_threshold_kb: 10240      # 10MB warning
        abort_threshold_kb: 102400    # 100MB abort
    partition_write_tombstone:
        warn_threshold: 1000          # Warn at 1k tombstones
        abort_threshold: 10000        # Block at 10k tombstones
```

---

## Code Patterns to Follow

### 1. Adding New Threshold Types:
- Create inner class in `TrackWarnings` (LongByteThreshold or IntThreshold)
- Add validation in `validate()` method
- Add getters/setters in `DatabaseDescriptor`
- Update `trackLargePartitionMutations()` to check new threshold

### 2. Integration at New Write Path:
```java
if (DatabaseDescriptor.getTrackWarningsEnabled()) {
    long sizeBytesWarn = DatabaseDescriptor.getPartitionWriteSizeWarnThresholdKb() * 1024;
    long sizeBytesAbort = DatabaseDescriptor.getPartitionWriteSizeAbortThresholdKb() * 1024;
    int tombstoneWarn = DatabaseDescriptor.getPartitionWriteTombstoneWarnThreshold();
    int tombstoneAbort = DatabaseDescriptor.getPartitionWriteTombstoneAbortThreshold();

    // For each partition being written:
    trackLargePartitionMutations(consistency, writeType, tableId, key,
                                sizeBytesWarn, sizeBytesAbort,
                                tombstoneWarn, tombstoneAbort);
}
```

### 3. Warning Message Format:
```java
String.format("Mutation to %s against partition %s triggered %s; " +
              "partition estimated %s is %d, but threshold was %d",
              tableMetadata, partitionKey,
              isAbort ? "abortion" : "warning",
              metricType, // "size" or "tombstone"
              estimatedValue, threshold)
```

---

## Files Modified Summary

### Core Implementation:
1. `src/java/org/apache/cassandra/config/Config.java` - Added TrackWarnings field
2. `src/java/org/apache/cassandra/config/DatabaseDescriptor.java` - Added accessors + validation
3. `src/java/org/apache/cassandra/config/TrackWarnings.java` - NEW: Configuration class
4. `src/java/org/apache/cassandra/exceptions/RequestFailureReason.java` - Added WRITE_TOO_LARGE
5. `src/java/org/apache/cassandra/exceptions/WriteAbortException.java` - NEW: Exception class
6. `src/java/org/apache/cassandra/exceptions/WriteFailureException.java` - Added constructors
7. `src/java/org/apache/cassandra/metrics/TopPartitionTracker.java` - Added lookup map
8. `src/java/org/apache/cassandra/service/StorageProxy.java` - Added tracking logic

### Test Infrastructure:
9. `test/distributed/org/apache/cassandra/distributed/test/HiddenTableTest.java` - NEW
10. `test/distributed/org/apache/cassandra/distributed/test/JavaDriverUtils.java` - Enhanced
11. `test/distributed/org/apache/cassandra/distributed/test/trackwarnings/PartitionWriteSizeWarningTest.java` - NEW
12. `test/unit/org/apache/cassandra/utils/AssertUtil.java` - Enhanced

---

## Next Steps / Future Work

Based on TODO comments in the code:
1. **Fix Token-Aware Routing**: Move validation to replica side for non-token-aware clients
2. **Write Type Detection**: Improve WriteType inference for mutations
3. **Estimate Accuracy**: Consider triggering actual partition size calculation when estimate is close to threshold
4. **Additional Metrics**: Could add tracking for other partition characteristics (wide rows, large cells, etc.)

---

## Key Takeaways

1. **Coordinator-side validation** using existing TopPartitionTracker infrastructure
2. **O(1) lookup performance** via added hash map in TopHolder
3. **Flexible threshold configuration** with separate warn/abort levels
4. **Client-visible warnings** via ClientWarn mechanism
5. **Fail-fast behavior** with WriteAbortException for abort thresholds
6. **Known limitation**: Requires token-aware routing to work correctly (bug noted in code)