# System Design

Felix is a sovereign-first, low-latency distributed data backend that unifies event streaming, message queueing, and distributed caching over a single QUIC-based transport layer.

## Design Principles

### 1. Sovereignty by Default

Each Felix cluster represents a **single sovereign region**. Data is isolated by default and cannot leave the region unless an explicit, configured bridge exists. This is enforced in routing, metadata, and encryption boundaries—not left to deployment discipline.

**Why this matters:**

- **Regulatory Compliance:** GDPR, CCPA, HIPAA require data residency
- **Data Sovereignty:** Government and enterprise data governance
- **Security:** Reduced attack surface with explicit data movement
- **Auditability:** Complete visibility into cross-region data flow

### 2. One Core Log, Many Semantics

Internally, Felix is built around a single append-only log abstraction. Different external semantics are projections over this core:

- **Streams (Pub/Sub):** Fanout cursors per subscription
- **Queues:** Shared consumer-group cursors with acknowledgements
- **Cache:** Key → latest value with TTL, backed by the same log for invalidation and replay

This drastically reduces operational complexity and consistency bugs compared to running Kafka, Redis, and a queueing system side-by-side.

### 3. Low-Latency First

Felix prioritizes predictable low latency over maximum batch throughput:

- **QUIC transport:** Multiplexed, encrypted, congestion-aware
- **Optional ephemeral streams:** No disk on hot path
- **Aggressive backpressure:** Bounded memory everywhere
- **Leader-based writes:** Tunable acknowledgement policies

### 4. Kubernetes-Native

Felix assumes Kubernetes for process lifecycle, identity (ServiceAccounts), networking and service discovery, and failure detection. Felix does **not** attempt to reimplement scheduling or node membership logic that Kubernetes already provides.

## System Architecture (Current MVP)

The current implementation is a single-node broker for development and testing:

```mermaid
flowchart TB
    subgraph Clients["Client Applications"]
        P1["Publisher 1"]
        P2["Publisher 2"]
        S1["Subscriber 1"]
        S2["Subscriber 2"]
        C1["Cache Client"]
    end

    subgraph Broker["Felix Broker (Single Node)"]
        direction TB
        Transport["QUIC Transport Layer<br/>felix-transport"]
        Wire["Wire Protocol Handler<br/>felix-wire framing"]
        Router["Stream Router<br/>Control vs Event vs Cache"]
        
        subgraph DataPlane["Data Plane"]
            PubSub["Pub/Sub Engine<br/>felix-broker"]
            Cache["Cache Engine<br/>TTL + eviction"]
            Storage["Ephemeral Storage<br/>In-memory"]
        end
        
        Metrics["Metrics Server<br/>:8080"]
        
        Transport --> Wire
        Wire --> Router
        Router --> PubSub
        Router --> Cache
        PubSub --> Storage
        Cache --> Storage
    end

    P1 & P2 --> Transport
    Transport --> S1 & S2
    C1 <--> Transport
    
    PubSub -.-> Metrics
    Cache -.-> Metrics
```

**Key Components:**

- **Transport Layer:** Accepts QUIC connections, manages stream lifecycle
- **Wire Protocol:** Frames messages, validates envelopes, routes by type
- **Pub/Sub Engine:** Enqueues publishes, manages subscriptions, fans out events
- **Cache Engine:** Handles put/get operations with TTL and lazy expiration
- **Storage:** In-memory ring buffers and hash maps (ephemeral)
- **Metrics Server:** Prometheus-compatible endpoint for monitoring

## Planned Multi-Node Architecture

The intended multi-node design adds explicit control-plane coordination and data-plane scalability:

```mermaid
flowchart TB
    subgraph Clients["Clients"]
        C1["Producers"]
        C2["Consumers"]
        C3["Cache Clients"]
    end
    
    LB["Load Balancer<br/>(L4 for QUIC)"]
    
    Clients --> LB
    
    subgraph ControlPlane["Control Plane (RAFT)"]
        direction LR
        CONTROLPLANE1["controlplane-0"]
        CONTROLPLANE2["controlplane-1"]
        CONTROLPLANE3["controlplane-2"]
        
        CONTROLPLANE1 <--> CONTROLPLANE2
        CONTROLPLANE2 <--> CONTROLPLANE3
        CONTROLPLANE1 <--> CONTROLPLANE3
        
        Meta["Metadata Store<br/>• Topics/Streams<br/>• Tenants/Namespaces<br/>• Shard Placement<br/>• ACLs/Quotas"]
    end
    
    subgraph DataPlane["Data Plane (Brokers)"]
        direction LR
        B1["Broker A<br/>Shards 0-99"]
        B2["Broker B<br/>Shards 100-199"]
        B3["Broker C<br/>Shards 200-299"]
    end
    
    subgraph Storage["Storage Layer"]
        direction LR
        Ephemeral["Ephemeral<br/>(in-memory)"]
        Durable["Durable Log<br/>(persistent volumes)"]
        Snapshots["Snapshots<br/>(object storage)"]
    end
    
    LB --> DataPlane
    ControlPlane --> Meta
    DataPlane <--> ControlPlane
    DataPlane --> Storage
```

### Control Plane Responsibilities

- **Metadata Management:** Topics, tenants, namespaces, ACLs
- **Shard Placement:** Assign shards to broker nodes
- **Health Monitoring:** Track broker liveness and readiness
- **Configuration:** Cluster-wide retention, limits, feature flags
- **Rebalancing:** Migrate shards on node failures or scaling events

### Data Plane Responsibilities

- **Client Connections:** Accept and route QUIC streams
- **Data Operations:** Publish, subscribe, cache operations
- **Shard Ownership:** Host assigned shards (leaders and followers)
- **Replication:** (Future) Replicate log entries to followers
- **Backpressure:** Enforce flow control and isolation

## Data Flow Patterns

### Publish/Subscribe Flow

```mermaid
sequenceDiagram
    participant P as Publisher
    participant B as Broker
    participant S1 as Subscriber 1
    participant S2 as Subscriber 2
    
    P->>B: Open control stream (QUIC bi)
    S1->>B: Open control stream (QUIC bi)
    S2->>B: Open control stream (QUIC bi)
    
    S1->>B: Subscribe(tenant, namespace, stream)
    B-->>S1: OK
    B->>S1: Open event stream (QUIC uni)
    
    S2->>B: Subscribe(tenant, namespace, stream)
    B-->>S2: OK
    B->>S2: Open event stream (QUIC uni)
    
    loop Publishing
        P->>B: Publish(batch of messages)
        B-->>P: ACK (optional)
        B->>B: Enqueue for fanout
        par Fanout to subscribers
            B->>S1: Event batch
            B->>S2: Event batch
        end
    end
```

**Key characteristics:**

- Publishers use bidirectional control streams for publish requests
- Subscribers get dedicated unidirectional event streams
- Fanout happens independently per subscriber (isolation)
- Batching is time and count-bounded for throughput optimization

### Cache Flow

```mermaid
sequenceDiagram
    participant C as Client
    participant B as Broker
    
    C->>B: Open cache stream pool (N connections)
    Note over C,B: M stream workers per connection
    
    par Concurrent requests
        C->>B: cache_put(tenant, namespace, cache, key1, value1, ttl)
        C->>B: cache_get(tenant, namespace, cache, key2)
        C->>B: cache_put(tenant, namespace, cache, key3, value3, ttl)
    end
    
    par Concurrent responses
        B-->>C: OK (key1)
        B-->>C: cache_value(key2, null)
        B-->>C: OK (key3)
    end
```

**Key characteristics:**

- Connection pooling reduces handshake overhead
- Request multiplexing over long-lived streams
- Request IDs for request/response matching
- Sub-millisecond latency at moderate concurrency

### Cross-Broker Routing (Planned)

When a client connects to a broker that doesn't own the target shard:

```mermaid
sequenceDiagram
    participant C as Client
    participant B1 as Broker (ingress)
    participant CONTROLPLANE as Control Plane
    participant B2 as Broker (shard owner)
    
    C->>B1: Publish(topic, batch)
    B1->>CONTROLPLANE: Lookup shard placement(topic)
    CONTROLPLANE-->>B1: owner = B2
    B1->>B2: Forward publish (internal QUIC)
    B2->>B2: Commit to log
    B2-->>B1: ACK
    B1-->>C: ACK
```

## Storage Architecture

### Ephemeral (Current MVP)

- **In-memory only:** No disk writes on hot path
- **Bounded buffers:** Ring buffers with fixed capacity
- **TTL support:** Lazy expiration on access
- **No persistence:** Data lost on restart

**Use cases:**

- Ultra-low latency workloads
- Development and testing
- Temporary caching
- Non-critical event streams

### Durable (Planned)

- **Write-Ahead Log (WAL):** Append-only log segments
- **Segmented storage:** Rotate segments based on time/size
- **Retention policies:** Time-based and size-based limits
- **Snapshots:** Periodic state snapshots for faster recovery

**Use cases:**

- Production event streaming
- Critical message delivery
- Long-term event retention
- Replay and audit trails

## Consistency Model

### Single-Node (MVP)

- **Delivery:** At-most-once (best-effort)
- **Ordering:** Per-stream ordering preserved per subscriber
- **Durability:** None (ephemeral only)

### Multi-Node (Planned)

**Tunable per stream:**

- **Leader-only acknowledgements:** Lowest latency, leader commits before replicating
- **Quorum acknowledgements:** Higher durability, waits for majority replica confirmation
- **Asynchronous replication:** Background replication after ACK
- **Synchronous replication:** Blocks on replication before ACK

**Delivery guarantees:**

- **At-least-once:** With durable storage and replay on failure
- **At-most-once:** Best-effort with no retries
- **Exactly-once:** (Future roadmap) via idempotent producers and transactions

## Multi-Region Architecture (Planned)

Felix enforces regional isolation with explicit bridges:

```mermaid
flowchart LR
    subgraph Region1["Region: US-EAST"]
        B1["Brokers<br/>US-EAST"]
        CONTROLPLANE1["Control Plane<br/>US-EAST"]
    end
    
    subgraph Region2["Region: EU-WEST"]
        B2["Brokers<br/>EU-WEST"]
        CONTROLPLANE2["Control Plane<br/>EU-WEST"]
    end
    
    subgraph Bridge["Explicit Bridge"]
        BridgeAgent["Bridge Agent<br/>• Allowlist<br/>• Encryption<br/>• Audit Log"]
    end
    
    B1 <-->|"Explicit config only"| BridgeAgent
    BridgeAgent <-->|"Explicit config only"| B2
    
    style Region1 fill:#e1f5ff
    style Region2 fill:#fff4e1
    style Bridge fill:#ffe1e1
```

**Bridge characteristics:**

- **Explicit Configuration:** No implicit data movement
- **Stream Allowlist:** Only specified streams replicate
- **Independent Encryption:** Per-region key contexts
- **Audit Trail:** Complete log of cross-region data movement
- **Compliance:** Satisfies strict data sovereignty requirements

## Scalability Considerations

### Vertical Scaling (Single-Node)

- **CPU:** More cores for parallel stream processing
- **Memory:** Larger buffers and cache capacity
- **Network:** Higher bandwidth for fanout
- **Typical:** 10k-50k msg/s on modern hardware

### Horizontal Scaling (Multi-Node)

- **Sharding:** Partition streams across brokers
- **Connection pooling:** Reuse connections across shards
- **Control plane:** RAFT quorum for metadata (3-5 nodes)
- **Data plane:** Many broker nodes for capacity
- **Target:** 100k-1M+ msg/s per cluster

## Next Steps

- [Components Deep Dive](components.md) - Detailed component architecture
- [Wire Protocol](wire-protocol.md) - Protocol specification
- [Semantics](semantics.md) - Delivery and consistency guarantees
- [Performance Tuning](../features/performance.md) - Optimize for your workload
