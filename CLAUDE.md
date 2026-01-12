# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Apache Cassandra 5.1 - A highly-scalable partitioned row store database built with Apache Ant. The main development branch is `trunk`.

## Build Commands

### Core Build Tasks
```bash
ant build              # Compile Cassandra classes
ant jar                # Build JAR files
ant artifacts          # Build distribution artifacts
ant clean              # Clean build artifacts
ant realclean          # Deep clean including generated files
ant check              # Pre-commit verification (code style, tests, docs)
```

### Code Quality
```bash
ant checkstyle         # Run checkstyle checks
ant rat-check          # Verify Apache license headers
```

## Testing Commands

### Unit Tests
```bash
ant test                              # Run all unit tests
ant testsome -Dtest.name=ClassName    # Run specific test class
ant testsome -Dtest.name=ClassName -Dtest.methods=methodName  # Run specific test method
```

### Other Test Types
```bash
ant long-test          # Long-running tests
ant burn-test          # Burn-in stress tests
ant stress-test        # Cassandra stress tests
ant jvm-dtest          # JVM-based distributed tests
ant microbench         # JMH microbenchmarks
```

### Test Scripts (Docker-based)
```bash
.build/run-tests.sh -a test              # Unit tests in Docker
.build/run-tests.sh -a long-test         # Long tests
.build/run-tests.sh -a jvm-dtest         # Distributed tests
.build/run-tests.sh -a simulator-test    # Simulator tests
.build/run-tests.sh -a dtest             # Python dtests
.build/run-python-dtests.sh dtest        # Python dtests directly

# Test splitting for parallel execution
.build/run-tests.sh -a test -c 1/64      # Run 1 of 64 splits

# Repeat tests to check for flakiness
.build/run-tests.sh -a test-repeat -t TestClass -e REPEATED_TESTS_COUNT=100
```

## Development Workflow

### Branch Naming
Branch names should follow: `your-name/jira-id/base-branch`
Example: `jsmith/CASSANDRA-12345/trunk`

### Commit Message Format
```
<One sentence description, usually Jira title>

<Optional lengthier description>

patch by <Authors>; reviewed by <Reviewers> for CASSANDRA-#####

Co-authored-by: Name <email>
```

### Working with Submodules (Accord)
```bash
# Switch all submodules to new branch
.build/sh/development-switch.sh --jira CASSANDRA-12345

# Commit changes to submodule and update reference
(cd modules/accord ; git commit -am 'changes')
.build/sh/bump-accord.sh

# Change and bump Accord
.build/sh/change-submodule-accord.sh
.build/sh/bump-accord.sh
```

## High-Level Architecture

### Storage Layer
- **SSTables**: Immutable sorted string tables using BTI (Big Trie Index) format
- **Memtables**: In-memory write buffers before flushing to SSTables
- **Commit Log**: Write-ahead log ensuring durability
- **Compaction**: UnifiedCompactionStrategy for background SSTable merging
  - See: `src/java/org/apache/cassandra/db/compaction/UnifiedCompactionStrategy.md`

### Query Processing (CQL3)
- CQL parser and executor in `src/java/org/apache/cassandra/cql3/`
- Query filtering and optimization in `src/java/org/apache/cassandra/db/filter/`
- Lightweight transactions using Paxos
- **Accord**: Distributed transaction consensus (separate submodule in `modules/accord/`)

### Distributed Coordination
- **TCM** (Transactional Cluster Metadata): Modern metadata management system
  - Replaces legacy Gossip-based schema distribution
  - Paxos-backed distributed log with epoch-based versioning
  - See: `src/java/org/apache/cassandra/tcm/TCM_implementation.md`
- **Gossip (GMS)**: Legacy peer discovery and failure detection in `src/java/org/apache/cassandra/gms/`
- **Streaming**: Data transfer between nodes in `src/java/org/apache/cassandra/streaming/`
- **Hints**: Hinted handoff for temporary node failures in `src/java/org/apache/cassandra/hints/`
- **Repair**: Anti-entropy repair mechanisms in `src/java/org/apache/cassandra/repair/`

### Secondary Indexing (SAI)
Storage-Attached Indexing in `src/java/org/apache/cassandra/index/sai/`
- Trie-based string indexes
- Block-oriented balanced trees for numeric indexes
- Row-based storage design
- See: `src/java/org/apache/cassandra/index/sai/README.md`

### Replication & Consistency
- **Locator**: Token-based data placement and replication strategies in `src/java/org/apache/cassandra/locator/`
- **Service**: Coordination of reads/writes with consistency levels in `src/java/org/apache/cassandra/service/`
- **Paxos**: Lightweight transactions for linearizable operations

### Core Data Structures
- **Tries**: In-memory and on-disk trie implementations in `src/java/org/apache/cassandra/db/tries/`
- **ByteComparable**: Efficient byte-comparable encoding
- **Compression**: Dictionary-based ZSTD compression support in `src/java/org/apache/cassandra/io/compress/`

## Key Source Directories

```
src/java/org/apache/cassandra/
├── db/                  # Database core (storage, compaction, commit log)
├── cql3/                # CQL query language implementation
├── io/sstable/          # SSTable format and operations
├── service/             # Core services and coordination
├── net/                 # Networking layer
├── streaming/           # Data streaming between nodes
├── repair/              # Anti-entropy repair
├── locator/             # Replication and placement strategies
├── tcm/                 # Transactional Cluster Metadata
├── auth/                # Authentication and authorization
├── schema/              # Schema management
├── index/sai/           # Storage-Attached Indexing
├── gms/                 # Gossip protocol
├── dht/                 # Distributed hash table
├── hints/               # Hinted handoff
├── tools/               # CLI tools (nodetool, etc.)
├── metrics/             # Metrics system
├── utils/               # Utility classes
├── transport/           # Client protocol (CQL native transport)
├── config/              # Configuration classes
└── journal/             # Accord journal integration
```

## Test Directory Structure

```
test/
├── unit/                # JUnit unit tests (primary test suite)
├── distributed/         # JVM distributed tests
├── long/                # Long-running tests
├── burn/                # Burn-in stress tests
├── memory/              # Memory-specific tests
├── microbench/          # JMH microbenchmarks
├── simulator/           # Simulation framework and tests
└── harry/               # Harry fuzz testing framework
```

## Testing Guidelines

From TESTING.md:

### Unit Tests
- Test all state transitions and branches
- Test that illegal states throw exceptions (use Guava preconditions)
- Test boundary conditions for ranges of values
- Use dependency injection to avoid global state
- Wrap global state access in protected methods for testability

### Integration Tests
- Test that messages are sent when expected
- Test that received messages have intended side effects
- Test dry start, restart, shutdown, and upgrade scenarios
- Mock out external dependencies but avoid mocking storage layer

### Distributed Tests (dtests)
- Black box tests for end-to-end cluster functionality
- Test client contracts
- Should have corresponding granular integration tests in Java

## Important Documentation

- `CONTRIBUTING.md` - Contribution guidelines and workflow
- `TESTING.md` - Comprehensive testing guidelines
- `src/java/org/apache/cassandra/tcm/TCM_implementation.md` - TCM architecture
- `src/java/org/apache/cassandra/db/compaction/UnifiedCompactionStrategy.md` - Compaction details
- `src/java/org/apache/cassandra/index/sai/README.md` - SAI indexing
- `src/java/org/apache/cassandra/io/sstable/SSTable_API.md` - SSTable API
- `src/java/org/apache/cassandra/db/memtable/Memtable_API.md` - Memtable API

## Command-Line Tools

Located in `bin/`:
- `cassandra` - Main server daemon
- `cqlsh` - Interactive CQL shell (Python-based)
- `nodetool` - Cluster management tool
- `sstableloader` - Bulk data loading
- `sstablescrub` - SSTable repair utility
- `sstableupgrade` - SSTable format upgrade
- `sstableverify` - SSTable validation

## Configuration Files

- `conf/cassandra.yaml` - Main Cassandra configuration
- `conf/jvm11-server.options` - JVM 11 options
- `conf/jvm17-server.options` - JVM 17 options
- `conf/cassandra-env.sh` - Environment variables
- `conf/logback.xml` - Logging configuration

## CI/CD

- **CircleCI**: Main CI platform (`.circleci/config.yml`)
- **Jenkins**: Kubernetes-based pipeline (`.jenkins/Jenkinsfile`)
- **GitHub Actions**: Code quality checks (`.github/workflows/code-check.yaml`)

## Java Version Support

- Default: Java 11
- Supported: Java 11 and 17
- All releases built with default JDK (Java 11)
- Check `build.xml` for `java.supported` property

## Accord Submodule

The `modules/accord/` directory is a Git submodule from `apache/cassandra-accord`. It provides distributed transaction consensus and is built separately with Gradle.