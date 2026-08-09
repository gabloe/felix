# How Felix Works

This guide explains Felix from first principles and then follows the current
implementation through the repository. It is intended for contributors who
want to understand not only the public API, but also which task owns each piece
of work, where data is queued, what is copied, what is shared, and what happens
under overload.

Code references use `path::symbol` rather than line numbers because symbols are
more stable as the implementation changes.

!!! important "Current implementation versus intended architecture"
    Felix currently has a highly optimized single-node data plane: QUIC
    transport, authenticated publish/subscribe, fanout, and an ephemeral cache.
    The repository also contains foundations for durable storage, routing,
    consensus, and multi-node operation, but those pieces are not all wired into
    the running broker. This guide explicitly distinguishes implemented behavior
    from planned behavior.

## 1. The shortest useful mental model

Felix is a brokered messaging and caching system:

- **Publishers** send byte payloads to named streams.
- **Subscribers** receive new payloads published to those streams.
- **Cache clients** store and retrieve values under scoped keys.
- A **broker** authenticates clients, validates resource scope, accepts traffic,
  and routes it through bounded queues.
- A **control plane** owns metadata and authorization state. Brokers periodically
  synchronize a local view of tenants, namespaces, streams, and caches.

The current pub/sub data path is:

```text
application
  -> felix-client Publisher
  -> client publish admission and worker queue
  -> QUIC bidirectional stream
  -> broker frame decoder and authorization
  -> broker publish admission and global worker queue
  -> felix-broker StreamState
  -> per-subscriber broker-core queue
  -> subscription lane
  -> per-connection writer
  -> QUIC unidirectional event stream
  -> client event router and subscription queues
  -> application
```

The important architectural boundary is:

- `crates/felix-broker/src/lib.rs` contains the transport-independent broker
  core.
- `services/broker/src/` turns that core into a network service.
- `crates/felix-client/src/` implements the client-side connection pools and
  APIs.
- `crates/felix-wire/src/lib.rs` defines what the two sides exchange.
- `crates/felix-transport/src/lib.rs` wraps the QUIC implementation.

## 2. Repository map

| Area | Responsibility | Start reading |
|---|---|---|
| `crates/felix-wire` | Frame header, protocol messages, binary fast paths | `crates/felix-wire/src/lib.rs` |
| `crates/felix-transport` | QUIC endpoint, connection, stream, flow-control, and UDP configuration | `crates/felix-transport/src/lib.rs` |
| `crates/felix-client` | Publisher, subscription, and cache client APIs | `crates/felix-client/src/client/client.rs` |
| `crates/felix-broker` | Stream registry, in-memory log, subscriber registry, fanout | `crates/felix-broker/src/lib.rs` |
| `crates/felix-storage` | Cache storage abstraction and ephemeral implementation | `crates/felix-storage/src/lib.rs` |
| `crates/felix-authz` | Token verification types and permission matching | `crates/felix-authz/src/lib.rs` |
| `services/broker` | Runnable broker, network handlers, auth, metrics, control-plane sync | `services/broker/src/main.rs` |
| `services/controlplane` | Metadata APIs, token exchange, JWKS, and RBAC | `services/controlplane/src/main.rs` |

Supporting crates such as `felix-consensus`, `felix-router`, and
`felix-metadata` represent the direction of the multi-node architecture. Do not
start with them when learning the current message path.

## 3. Networking foundations: UDP, TLS, and QUIC

### 3.1 What UDP provides

UDP sends independent datagrams between network addresses. It is lightweight,
but by itself it does not guarantee:

- delivery;
- ordering;
- retransmission;
- congestion control;
- connection identity; or
- encryption.

An application built directly on UDP must implement those properties itself if
it needs them.

### 3.2 What QUIC adds

QUIC is a secure transport protocol implemented over UDP. It adds:

- a connection handshake;
- TLS 1.3 encryption and peer authentication;
- reliable delivery and retransmission;
- congestion control;
- connection-level and stream-level flow control;
- multiple logical byte streams inside one connection; and
- ordered delivery within each stream.

Unlike TCP, independent QUIC streams do not share one connection-wide ordered
byte sequence. A lost packet affecting one stream does not require unrelated
streams to wait for that stream's missing bytes. This is one reason Felix can
use one connection for several independent publish or cache workers.

QUIC does **not** make all application work parallel automatically. Within one
QUIC stream, bytes are still ordered, and Felix deliberately assigns one writer
task to each stream. The number of connections and streams therefore controls
real application parallelism.

Felix uses the [`quinn`](https://github.com/quinn-rs/quinn) Rust implementation.
The wrapper types are:

- `crates/felix-transport/src/lib.rs::QuicServer`
- `crates/felix-transport/src/lib.rs::QuicClient`
- `crates/felix-transport/src/lib.rs::QuicConnection`

`QuicServer::bind` and `QuicClient::bind` create UDP sockets, install Quinn's
transport configuration, and create endpoints using `quinn::TokioRuntime`.

### 3.3 Connections and streams

A QUIC **connection** is the encrypted relationship between a client endpoint
and a server endpoint. A connection contains many streams:

- A **bidirectional stream** has one send direction and one receive direction.
  Either peer may send data on its half.
- A **unidirectional stream** carries bytes in one direction only.

Felix maps its operations onto these primitives:

| Operation | Stream type | Why |
|---|---|---|
| Authentication and acknowledged publish | Bidirectional | Client sends requests; broker sends responses |
| Subscribe setup | Bidirectional | Client sends `Subscribe`; broker returns `Subscribed` or `Error` |
| Event delivery | Broker-opened unidirectional | Events flow only from broker to subscriber |
| Cache put/get | Bidirectional | Each operation is a request/response exchange |
| Optional high-throughput publish ingress | Client-opened unidirectional | Fire-and-forget traffic does not need responses |

The current Rust client opens authenticated bidirectional workers for its normal
publish API. The broker also supports unidirectional publish ingress in
`services/broker/src/transport/quic/streams/uni.rs::run_uni_loop`.
Fire-and-forget means that the broker does not return a publish response; it
does not mean that the stream is unauthenticated. A unidirectional publish
stream must begin with `Message::Auth` before it sends publish frames.

### 3.4 Transport tuning

`crates/felix-transport/src/lib.rs::TransportConfig` controls:

- maximum concurrent streams;
- connection and per-stream flow-control windows;
- send window;
- initial MTU and path-MTU discovery;
- maximum accepted UDP payload;
- UDP socket send and receive buffers; and
- an optional initial congestion window.

`TransportConfig::quinn_transport_config` translates these settings into Quinn
configuration. `TransportConfig::bind_udp_socket` applies socket buffer sizes
best-effort, reducing the requested size until the host accepts it.

These settings matter because high-rate messaging can otherwise become limited
by tiny flow-control windows, excessive small datagrams, or kernel UDP drops.
They improve capacity; they do not change Felix's delivery semantics.

## 4. Felix's wire protocol

QUIC provides reliable byte streams, but it does not define where one Felix
message ends and another begins. Felix adds its own framing protocol in
`crates/felix-wire/src/lib.rs`.

### 4.1 Frame envelope

Every Felix frame begins with `FrameHeader`, a 12-byte header:

```text
u32 magic    = 0x464C5831 ("FLX1")
u16 version  = 1
u16 flags
u32 payload length
payload bytes...
```

`FrameHeader::decode` rejects an invalid magic value, unsupported version, or
incomplete header before the payload is trusted. Higher-level read helpers also
enforce configured frame-size limits.

### 4.2 JSON control messages

`crates/felix-wire/src/lib.rs::Message` is the version-one protocol enum. With
zero flags, `Message::encode` serializes a message as JSON and places it in a
frame. JSON is used where flexibility and request metadata matter more than
minimum encoding overhead:

- `Auth`
- acknowledged `Publish` and `PublishBatch`
- `Subscribe` and `Subscribed`
- `EventStreamHello`
- cache request/response messages
- acknowledgements and errors

Payload bytes inside JSON messages are Base64 encoded.

### 4.3 Binary data-plane frames

The hot paths avoid JSON:

- `FLAG_BINARY_PUBLISH_BATCH` identifies a binary publish batch.
- `FLAG_BINARY_EVENT_BATCH` identifies the older event format that includes a
  subscription ID.
- `FLAG_BINARY_EVENT_BATCH_SHARED` identifies the current shared event format.

The binary publish encoder
`felix_wire::binary::encode_publish_batch_bytes_with_stats` writes the resource
names, item count, and length-prefixed payloads directly into a byte buffer.

The shared event encoder
`felix_wire::binary::encode_shared_event_batch_bytes` writes only:

```text
u32 payload_count
repeated:
  u32 payload_length
  payload bytes
```

It does not repeat tenant, namespace, stream, or subscription identifiers. The
preceding `EventStreamHello` has already bound that QUIC stream to one
subscription.

## 5. Felix's resource model

Pub/sub resources are scoped as:

```text
tenant -> namespace -> stream
```

Cache resources are scoped as:

```text
tenant -> namespace -> cache -> key
```

The broker keeps local registries for these objects in
`crates/felix-broker/src/lib.rs::Broker`. A stream does not become valid merely
because a client names it; it must exist in the broker's synchronized metadata.

The running service obtains metadata from the control plane through
`services/broker/src/controlplane.rs::start_sync`. On cold start,
`sync_once` fetches full snapshots in dependency order:

1. tenants;
2. namespaces;
3. caches;
4. streams.

Later iterations consume incremental change feeds. Each resource type has its
own sequence cursor. A failed fetch is retried without advancing that cursor,
so synchronization is eventually consistent rather than transactional across
all resource types.

## 6. Broker startup and process lifecycle

The executable starts at `services/broker/src/main.rs::main`, which calls
`run_with_shutdown`.

Startup proceeds in this order:

1. `observability::init_observability` installs tracing and the Prometheus
   recorder.
2. `BrokerConfig::from_env_or_yaml` resolves configuration.
3. `Broker::new(EphemeralCache::new().into())` creates the in-process broker
   core with an ephemeral cache backend.
4. `BrokerAuth::new` creates the broker's control-plane-backed authenticator.
5. `observability::serve_metrics` starts the health and metrics HTTP server.
6. `build_server_config` creates the QUIC TLS configuration.
7. `QuicServer::bind` binds the UDP/QUIC listener.
8. `quic::serve_with_shutdown` starts the connection accept loop.
9. `controlplane::start_sync` starts metadata synchronization.

!!! warning "Current TLS certificate behavior"
    `services/broker/src/main.rs::build_server_config` currently generates a
    fresh self-signed certificate for `localhost`. The code explicitly marks
    this as development behavior, not production certificate management.

### 6.1 Graceful shutdown

Shutdown is not a single task abort. `run_with_shutdown` performs an ordered
drain:

1. mark readiness as draining, so load balancers stop routing here while the
   broker can still serve;
2. cancel the QUIC accept loop so no new connections are admitted;
3. wind down the connections already accepted, and wait for them under one
   deadline;
4. stop control-plane synchronization;
5. stop the metrics server last.

Keeping metrics available during the drain allows operators to observe what is
still in flight. The shared deadline is implemented through
`felix_common::lifecycle::DrainBudget`, and it is a *single* budget spanning
every subsystem rather than a timeout per subsystem, so total shutdown time
stays bounded no matter how many things are slow.

Step 3 is the subtle one. The same cancellation token reaches every connection
task, and `handle_connection_with_shutdown` responds by:

1. no longer accepting new streams on that connection;
2. giving the streams already in flight a bounded grace period — half the
   process drain budget — to finish; and
3. closing the QUIC connection with CONNECTION_CLOSE, so the peer learns this
   was a deliberate shutdown rather than a server that vanished.

The grace has to be bounded because control and subscription streams are
long-lived by design: a publisher holds one open and streams requests down it, a
subscriber holds one open to receive events. Neither ends until the *client*
closes it. Waiting on them unconditionally means shutdown never completes
cooperatively — it burns the entire deadline and then force-aborts, dropping the
in-flight work the drain exists to protect. That was the behavior the soak
harness measured before this was fixed (see §16.1).

## 7. Client connection architecture

`crates/felix-client/src/client/client.rs::Client::connect_with_transport`
constructs three separate pools.

### 7.1 Publish pool

For each configured publish connection, the client opens several
bidirectional streams. Each stream:

1. is authenticated by `authenticate_stream`;
2. receives a bounded `mpsc` queue; and
3. gets one `run_publisher_writer` task that exclusively owns its Quinn
   `SendStream` and `RecvStream`.

The single-writer ownership is intentional. It avoids interleaved writes and
preserves the enqueue order of requests assigned to that worker.

### 7.2 Cache pool

The cache pool also uses multiple connections and multiple bidirectional
streams per connection. Every stream has a `run_cache_worker` task. Each worker
performs sequential request/response exchanges, while separate workers allow
independent cache operations to progress concurrently.

### 7.3 Event pool

Event connections are reserved for subscriptions. Each event connection gets
one `event_router::run_event_router` task that accepts broker-opened
unidirectional streams and associates them with subscription IDs.

Separate pools allow publish, cache, and event-delivery flow control to be
tuned independently and keep one traffic class from consuming all streams in
another.

## 8. Authentication and authorization

Every client-created control stream begins with authentication. The broker's
read loop is `services/broker/src/transport/quic/streams/control.rs::run_control_loop`.

Before authentication:

- JSON traffic must be `Message::Auth`.
- Binary publish frames are rejected with `auth required`.

`services/broker/src/auth.rs::BrokerAuth::authenticate`:

1. ensures the tenant's JWKS is cached;
2. verifies the token signature and critical claims;
3. verifies tenant scope;
4. compiles token permissions into a `PermissionMatcher`; and
5. returns an `AuthContext`.

After authentication, the control loop authorizes each resource operation with
the corresponding action and scoped resource:

- stream publish;
- stream subscribe;
- cache get; or
- cache put.

The control plane is the authority for token exchange, public verification
keys, and RBAC policy. The broker caches enough state to verify and authorize
the data path locally instead of calling the control plane for every message.

## 9. The complete publish path

This is the central path to understand.

### 9.1 Application to `Publisher`

The application obtains a handle using
`felix_client::Client::publisher`, then calls:

- `Publisher::publish`; or
- `Publisher::publish_batch`.

In `crates/felix-client/src/client/publisher.rs`:

- `AckMode::None` selects binary encoding.
- `AckMode::PerMessage` and `AckMode::PerBatch` currently select JSON because
  binary acknowledgement framing has not been negotiated.

### 9.2 Client worker selection

`Publisher::select_worker` supports:

- `PublishSharding::RoundRobin`, which distributes calls across workers; and
- `PublishSharding::HashStream`, which consistently maps
  `(tenant, namespace, stream)` to one worker.

Hashing a stream to one worker preserves client-side order for that stream and
avoids moving it between QUIC streams. A bounded `StreamShardCache` avoids
rehashing frequently used stream names.

Round-robin can provide more parallelism, but publishing the same logical
stream through several workers weakens the simple per-client ordering model.

### 9.3 Client byte admission

Before enqueueing, `PublishAdmission::acquire` reserves permits equal to the
estimated or encoded frame size from a byte-counting semaphore.

This is distinct from the worker channel's item capacity:

- the channel limits the number of queued requests;
- the semaphore limits the number of queued or processing bytes.

The `OwnedSemaphorePermit` is stored inside `PublishRequest`. It remains held
until the writer has processed the request, so the budget represents resident
work rather than only admission-time work.

### 9.4 Client encoding and stream writer

For unacknowledged traffic,
`Publisher::publish_batch_binary` calls
`felix_wire::binary::encode_publish_batch_bytes_with_stats`, then enqueues
`PublishRequest::BinaryBytes`.

`run_publisher_writer` is the only task writing to that publish stream. For
JSON requests it constructs the frame in reusable scratch storage. For binary
requests it writes the already encoded bytes.

For acknowledged traffic, the writer waits for a response and verifies the
request ID. For unacknowledged traffic, the public call completes after the
client writer has handed the frame to QUIC; it does not receive broker
confirmation.

### 9.5 Broker connection and stream handling

`services/broker/src/transport/quic/conn.rs::serve_with_shutdown` accepts QUIC
connections and tracks each connection task.

`handle_connection` concurrently accepts:

- bidirectional streams, dispatched to
  `transport/quic/streams/handlers.rs::handle_stream`; and
- unidirectional streams, dispatched to
  `transport/quic/streams/handlers.rs::handle_uni_stream`.

For a bidirectional stream, `handle_stream` creates:

- one outbound response queue;
- one `run_writer_loop` task owning the stream's `SendStream`;
- an optional commit-ack waiter task;
- cancellation and throttling channels; and
- a per-stream cache of resolved stream handles.

The current task runs `run_control_loop` over incoming frames.

### 9.6 Decode and authorize

`run_control_loop` examines frame flags before JSON decoding. A binary publish
batch goes directly to `handle_binary_publish_batch_control`.

JSON `Publish` and `PublishBatch` messages are decoded into `Message`, checked
against the authenticated tenant and permission matcher, and passed to the
corresponding publish handler.

### 9.7 Resolve the stream once

The broker transport converts the textual stream identity into
`felix_broker::StreamHandle` through
`handlers/publish.rs::resolve_stream_cached`.

A `StreamHandle` is a cheap `Arc<StreamState>` plus a dense numeric ID. Once
resolved:

- the worker can be selected with `handle.id() % worker_count`;
- the hot path avoids repeatedly hashing three strings; and
- it avoids repeatedly reading the shared stream registry.

The transport cache has a TTL so metadata changes can eventually invalidate
old resolutions. Removed stream states are also marked inactive, and
`Broker::publish_batch_to_handle` checks that bit before publishing.

### 9.8 Broker byte and item admission

The network handler constructs a `PublishJob` and calls
`services/broker/src/transport/quic/handlers/publish.rs::enqueue_publish`.

Two byte budgets are acquired:

1. a per-connection budget, preventing one publisher connection from occupying
   the entire broker;
2. a process-wide budget shared by all publish workers.

The job also enters a bounded worker channel. `EnqueuePolicy` determines what
happens when capacity is unavailable:

- `Drop`: shed fire-and-forget traffic and increment drop counters;
- `Fail`: reject acknowledged traffic immediately;
- `Wait`: wait up to a configured timeout, propagating backpressure.

The permits remain attached to the `PublishJob` until the broker worker
finishes it.

### 9.9 Global stream-sharded workers

`services/broker/src/transport/quic/conn.rs::build_publish_context` creates a
process-wide worker pool. It is deliberately not one pool per connection:
per-connection pools previously multiplied concurrent access to shared stream
state and increased contention.

Every resolved stream handle maps to one worker. Therefore publishes to the
same stream are serialized inside the broker even when they arrive through
different connections.

With `core_shards` enabled, there is exactly one publish worker per shard and
worker `i` runs on shard runtime `i`.

### 9.10 Broker core append and fanout

The worker calls
`crates/felix-broker/src/lib.rs::Broker::publish_batch_to_handle`.

That function:

1. verifies that the handle is active;
2. calls `StreamState::append_batch`;
3. loads the current subscriber snapshot;
4. creates one `DeliveryEnvelope`; and
5. enqueues a clone of that envelope to each subscriber.

`append_batch` takes the stream log mutex once for the whole batch, assigns
monotonic sequence numbers, appends `Bytes` clones, and trims the oldest
entries to the configured in-memory capacity.

The subscriber registry itself is protected by a mutex because subscriptions
are added and removed. The publish hot path does not take that mutex:
`StreamState` maintains an `ArcSwap<Vec<SubscriberEntry>>` snapshot. Subscribe
and unsubscribe rebuild the snapshot; publish loads it lock-free.

### 9.11 What is copied during fanout

`DeliveryEnvelope` contains:

- `Arc<[Bytes]>` for the payload batch;
- the enqueue timestamp; and
- `Mutex<Option<Bytes>>` for a lazily cached encoded event frame.

Cloning an envelope for ten subscribers increments reference counts. It does
not clone every payload buffer and does not encode ten event frames.

The first subscriber feeder calling `DeliveryEnvelope::shared_event_frame`
performs `encode_shared_event_batch_bytes` and stores the result. Other feeders
receive cheap `Bytes` clones of the same encoded frame.

This reuse applies when the envelope already contains multiple payloads, and in
the forced single-event mode. In the ordinary one-payload batching path, each
subscriber feeder may combine that payload with later envelopes according to
its own timing and then encode its resulting batch. That path can therefore
perform more than one event-frame encode per original publish. For multi-item
publish batches—the important throughput case—the shared envelope normally
reduces serialization from one encode per subscriber to one encode per publish
batch.

## 10. Acknowledgement semantics

`felix_wire::AckMode` has `None`, `PerMessage`, and `PerBatch`.

The important distinction is broker configuration:

- With `ack_on_commit = false`, an acknowledgement means the broker accepted
  the job into its ingress queue.
- With `ack_on_commit = true`, the broker waits until the publish worker
  completes `publish_batch_to_handle`.

In the current single-node broker, "commit" means the in-memory append and
fanout operation completed. It does **not** mean a durable disk write or
replication quorum.

Commit acknowledgements use:

- `handlers/publish.rs::AckWaiterMessage`;
- a bounded waiter semaphore; and
- `streams/ack_waiter.rs::run_ack_waiter_loop`.

Acknowledgements carry request IDs and may be emitted out of order, allowing
independent completed jobs to respond without waiting for an earlier slow job.

Commit acknowledgement is not an exactly-once outcome protocol. The publish job
is enqueued before the broker reserves and submits all acknowledgement-waiter
state. If that later step is overloaded, or if `ack_wait_timeout` expires, the
client can receive an overload or commit-timeout error even though the publish
was already enqueued and may have completed. A client must not interpret every
acknowledgement error as proof that the event was not published.

## 11. The complete subscription and fanout path

### 11.1 Client subscribe request

`crates/felix-client/src/client/client.rs::Client::subscribe`:

1. checks that the requested tenant matches the client's authenticated tenant;
2. selects an event connection round-robin;
3. opens and authenticates a bidirectional control stream;
4. sends `Message::Subscribe` with no client-assigned ID;
5. finishes its send half;
6. reads `Message::Subscribed`; and
7. registers the returned ID with the event router.

The broker assigns the ID. This is essential because independent `Client`
instances otherwise start local counters at the same values and can collide.

### 11.2 Broker subscription registration

The control loop authorizes the request and calls
`services/broker/src/transport/quic/handlers/subscribe.rs::handle_subscribe_message`.

That function:

1. allocates a globally unique subscription ID if none was supplied;
2. reserves capacity in the connection's `SubscriptionLimiter`;
3. calls `Broker::subscribe`;
4. opens a new broker-to-client unidirectional stream;
5. writes `EventStreamHello { subscription_id }`;
6. selects a writer lane;
7. registers the event stream with that lane;
8. sends `Subscribed` only after registration succeeds; and
9. spawns `run_lane_feeder`.

The acknowledgement therefore means that the broker-core queue and event
writer pipeline are both ready, not merely that the request was parsed.

### 11.3 Broker-core subscriber queue

`Broker::subscribe` resolves the stream and calls
`StreamState::register_subscriber`.

Registration creates a bounded `mpsc::channel<DeliveryEnvelope>`, stores its
sender in a `Slab`, rebuilds the publish snapshot, and returns a
`SubscriptionReceiver`.

Dropping the accompanying `SubscriptionGuard` removes the registry entry and
rebuilds the snapshot.

### 11.4 Event stream routing on the client

The broker may open the unidirectional event stream before or after the client
has processed `Subscribed`. Therefore
`crates/felix-client/src/client/event_router.rs::run_event_router` maintains two
bounded maps:

- registrations waiting for streams;
- streams waiting for registrations.

It reads `EventStreamHello` from each new unidirectional stream and joins the
two sides by subscription ID.

### 11.5 Lane feeder

`run_lane_feeder` receives `DeliveryEnvelope`s from the broker-core queue. It
either:

- reuses the envelope's shared encoded frame;
- splits an oversized envelope into bounded frames; or
- coalesces single-event envelopes until an event count, byte count, or timer
  limit is reached.

It then sends `LaneCommand::Delivery` to the selected writer lane.

When core sharding is enabled, the feeder is spawned on the same shard that
owns the stream's publish worker. The enqueue and dequeue sides of the
broker-core subscriber channel therefore stay core-local.

### 11.6 Writer lanes

`WriterLaneManager` owns a configurable set of bounded lane queues. Lane
`subscriber_single_writer_per_conn` is checked first; when enabled, every
subscriber on one connection is forced onto that connection's lane. Otherwise,
lane assignment follows `SubscriberLaneShard`:

- `Auto` currently hashes the subscription ID;
- `SubscriberIdHash` explicitly hashes the subscription ID;
- `ConnectionIdHash` hashes the connection ID when one is available; or
- `RoundRobinPin` assigns a stable lane once at subscription time.

`run_writer_lane` performs little actual I/O. It receives lane commands and
forwards them to the writer for the QUIC connection that owns the subscription.

The lane layer bounds parallelism and separates broker-core fanout from
connection-specific scheduling.

### 11.7 Connection writer

`run_connection_writer` owns the actual `SendStream`s for every subscription on
one QUIC connection.

For each subscriber it preserves at most one in-flight write. Across different
subscribers it uses `FuturesUnordered`, so writes proceed concurrently:

- subscriber A may be blocked by QUIC flow control;
- subscriber B can complete;
- B can begin its next write without waiting for A.

This continuous pipeline avoids a round barrier where the slowest subscriber
would delay every other subscriber sharing the connection.

### 11.8 Client subscription pipeline

Once the event router supplies the `RecvStream`,
`Subscription::spawn_pipeline` creates two tasks:

1. `run_subscription_io_task` reads complete Felix frames from QUIC into a
   bounded frame queue.
2. `run_subscription_dispatch_task` decodes those frames and places individual
   payloads into a bounded event queue.

`Subscription::next_event` receives one payload and returns an `Event` carrying
the subscription's tenant, namespace, and stream identity.

For `FLAG_BINARY_EVENT_BATCH_SHARED`, dispatch uses
`felix_wire::binary::decode_shared_event_batch`. The subscription identity does
not need to be present in each batch because the QUIC stream was already bound
by `EventStreamHello`.

## 12. Ordering guarantees

Ordering must be described at a specific boundary:

- One client publish worker writes requests in queue order.
- `HashStream` keeps one logical stream on one client worker.
- The broker maps one `StreamHandle` to one global publish worker.
- `StreamState::append_batch` assigns sequence numbers in worker processing
  order.
- Each subscriber receives envelopes through one ordered broker-core channel.
- Lane and connection writers preserve ordering for each subscriber.
- QUIC preserves byte order within the subscriber's event stream.

There is no universal order across different streams.

When several independent publishers publish concurrently to the same stream,
the resulting order is the order in which their jobs reach and are dequeued by
the stream's broker worker. Felix cannot infer a stronger application-level
causal order between independent producers.

## 13. Backpressure and overload

Felix uses bounded queues instead of allowing memory use to grow without limit.
There are six main checkpoints in publish-to-delivery order:

| # | Checkpoint | Bounds |
|---:|---|---|
| 1 | Client `PublishAdmission` | In-flight publish bytes across client workers |
| 2 | Client publish worker channel | Queued publish items per worker |
| 3 | Broker publish admission | Per-connection and process-wide publish bytes |
| 4 | Broker publish worker channel | Queued publish jobs |
| 5 | Broker-core subscriber channel | Envelopes waiting for one subscriber |
| 6 | Writer lane/connection queues | Encoded deliveries waiting for QUIC writers |

QUIC flow control is the final transport-level checkpoint beneath these.

### 13.1 Block versus drop

Broker subscriber queues use `felix_broker::SubQueuePolicy`:

- `Block` waits for capacity;
- `DropNew` discards the new item when full;
- `DropOld` is currently accounted separately but implemented as drop-new
  behavior.

The writer lane has its own independent policy because a subscriber can have
space in its broker-core queue while its shared connection writer is saturated.

Production defaults favor bounded latency and visible drops. Lossless benchmark
profiles select blocking queues and `pub_ingress_wait`, allowing pressure to
propagate backward until publishers slow down.

Neither policy is universally correct:

- dropping isolates healthy publishers and subscribers from a slow consumer;
- blocking preserves delivery but can let one slow subscriber throttle every
  producer of that stream.

### 13.2 Why both byte limits and item limits exist

A queue depth of 64 does not express how much memory 64 jobs consume. Jobs may
contain tiny payloads or multi-megabyte batches.

Felix therefore uses:

- item-count channels for scheduler and queue bounds; and
- byte-counting semaphores for resident payload bounds.

The permit travels with the work and is released after processing.

## 14. Cache request path

The public methods are:

- `Client::cache_put`;
- `Client::cache_get`.

Each call:

1. allocates a request ID;
2. selects a cache worker round-robin;
3. enqueues a `CacheRequest`; and
4. waits on a one-shot response channel.

`crates/felix-client/src/client/cache.rs::run_cache_worker` owns one
bidirectional QUIC stream and performs sequential round trips:

```text
encode -> write -> read -> decode -> validate request ID
```

Different cache workers execute concurrently. One worker remains sequential so
response matching is simple and the stream has one writer.

The broker control loop authorizes `CachePut` or `CacheGet`, then calls the
broker's `StorageApi`.

### 14.1 Ephemeral cache implementation

`crates/felix-storage/src/lib.rs::StorageApi` defines `put`, `get`, `delete`,
`len`, and `is_empty`.

The running broker uses
`crates/felix-storage/src/ephemeral_cache.rs::EphemeralCache`, which stores
entries in an async `RwLock<HashMap<CacheKey, CacheEntry>>`.

TTL behavior is lazy:

- `put` computes `expires_at = Instant::now() + ttl`;
- `get` checks the deadline;
- an expired entry is removed and returned as a miss.

There is no background expiration sweeper. A never-read expired key remains in
the map until another operation removes it. `EphemeralCache` contains an
optional capacity limit whose current eviction implementation removes an
arbitrary key rather than using LRU, but the running broker constructs
`EphemeralCache::new()` with no maximum entry count. Capacity eviction is
therefore inactive in the current service.

## 15. Core sharding and CPU ownership

Tokio's normal multi-threaded runtime may run a task on different worker
threads over time. That is flexible, but a hot stream can pay for:

- cross-core cache-line movement;
- channel wakeups between cores; and
- scheduler migration.

`services/broker/src/core_shards.rs::CoreShards` creates dedicated
single-threaded Tokio runtimes. On Linux, `pin_to_core` uses
`sched_setaffinity` to pin each runtime thread to a CPU.

A stream handle selects its owner:

```text
shard = handle_id % shard_count
```

The same mapping is used for:

- its broker publish worker; and
- its subscription lane feeders.

Append, fanout enqueue, and feeder dequeue therefore occur on one core. QUIC I/O
remains on the main runtime because Quinn's endpoint driver performs
packetization, encryption, and socket I/O independently.

Core sharding scales across **streams**, not within one stream. A workload with
one logical stream still has one owning shard by design.

## 16. Observability

`services/broker/src/observability.rs::init_observability` installs tracing,
OpenTelemetry propagation, and a Prometheus recorder.

The broker exposes:

- health and readiness endpoints;
- Prometheus metrics;
- queue depth and drop counters;
- publish-stage timings;
- subscriber lane and connection-writer timings; and
- optional frame counters.

Hot-path timing code is feature-gated. The relevant modules are:

- `services/broker/src/timings.rs`
- `services/broker/src/timings_telemetry.rs`
- `crates/felix-broker/src/timings.rs`
- `crates/felix-client/src/timings.rs`
- `services/broker/src/transport/quic/telemetry.rs`

When investigating missing messages, begin with:

- broker ingress drop/rejection counters;
- `felix_subscribe_dropped_total`;
- subscriber lane drop counters;
- client subscription queue drop counters; and
- QUIC connection close/write-error logs.

A nonzero drop counter indicates an intentional overload policy before it
indicates a routing bug.

### 16.1 The soak harness

`services/broker/src/bin/soak/` is the resource-leak and lifecycle harness. Run
it with:

```bash
cargo run --release -p broker --bin soak -- --duration-secs 60
```

It stands up a real broker with real QUIC connections and the real auth path,
then drives five phases: sustained load, connection churn, slow subscribers
saturating their queues, repeated identical load cycles, and repeated
`SIGTERM` restarts of a genuine child process. It samples RSS and open file
descriptors throughout, scrapes the broker's own gauges after quiescence, and
exits non-zero on a finding.

Two of its design choices are worth knowing before you read the output:

- **Memory is judged across repeated identical cycles, not against a baseline.**
  Comparing post-load RSS to pre-load RSS only measures allocator retention —
  allocators do not return freed pages promptly, so that comparison flags every
  healthy run. A real leak shows up as peak RSS still climbing on the last
  identical cycle, where retention plateaus.
- **File descriptors are the sharpest signal.** Every leaked connection or socket
  appears there, and unlike RSS there is no caching behavior to explain growth
  away, so the fd check is exact rather than tolerance-based.

The gauges it asserts must return to zero — `felix_sub_active_connections`,
`felix_sub_connection_subscribers`, `felix_broker_ingress_queue_depth`,
`felix_broker_out_ack_depth` — are the registration counters. Anything left in
them after every client has disconnected is an entry that will never be
reclaimed.

Findings and the current steady-state envelope are recorded in
`docs/security/soak-report.md`, alongside the panic audit in
`docs/security/panic-audit.md`. Both are repository-local audit records rather
than published pages.

## 17. What is implemented today

The current running system includes:

- encrypted QUIC transport;
- framed JSON and binary protocol paths;
- authenticated and authorized client streams;
- pooled publisher, subscriber, and cache connections;
- stream-sharded publish workers;
- an in-memory per-stream replay log in broker core;
- bounded per-subscriber queues;
- shared encode-once fanout;
- writer lanes and pipelined per-connection delivery;
- ephemeral cache storage with TTL;
- control-plane metadata synchronization;
- metrics, tracing, and optional detailed timings;
- graceful broker shutdown; and
- optional Linux core pinning.

## 18. What is partial or planned

Do not assume the following are complete production paths:

- **Durable pub/sub storage:** `felix-storage` contains log and tiered-storage
  foundations, but the running broker service uses the broker core's bounded
  in-memory log and `EphemeralCache`.
- **Public cursor replay:** broker core provides `cursor_tail` and
  `subscribe_with_cursor`, but replay is not yet a complete client-to-broker
  transport feature.
- **Hashed pooled subscription streams:** configuration exists, but
  `handle_subscribe_message` currently records a fallback and uses one
  unidirectional stream per subscriber.
- **Multi-node replication and consensus:** relevant crates and design
  documents exist, but they are not the current data path described here.
- **Multi-region routing and residency enforcement:** these remain broader
  architectural work.
- **Production certificate provisioning:** the broker currently generates a
  development self-signed certificate.
- **Per-subsystem shutdown cancellation:** the drain cancels admission and winds
  connections down under a bounded grace, but publish workers, acknowledgement
  waiters, and subscription writers are not individually signalled to stop.
  In-flight work inside the grace window completes; work still running when the
  grace expires is ended by closing the connection.

## 19. A worked example

Assume one application publishes a binary batch of 64 payloads to
`tenant-a/orders/updates`, with ten subscribers.

1. `Publisher::publish_batch` sees `AckMode::None` and selects the binary path.
2. `Publisher::select_worker` hashes the stream to one client publish worker.
3. The batch is encoded once into a binary Felix frame.
4. Client `PublishAdmission` reserves the encoded byte count.
5. The request enters that worker's bounded channel.
6. `run_publisher_writer` writes the bytes to its authenticated QUIC stream.
7. The broker control loop recognizes `FLAG_BINARY_PUBLISH_BATCH`.
8. The broker verifies the authenticated tenant and publish permission.
9. `resolve_stream_cached` obtains the stream's `StreamHandle`.
10. Broker per-connection and global byte admission reserve the payload bytes.
11. `enqueue_publish` sends the job to `handle.id() % worker_count`.
12. The worker calls `Broker::publish_batch_to_handle`.
13. `StreamState::append_batch` assigns 64 sequence numbers under one lock.
14. The broker loads the lock-free subscriber snapshot containing ten senders.
15. One `DeliveryEnvelope` is created and cloned into ten subscriber queues.
16. The first awakened feeder encodes one shared event frame; the other nine
    clone the cached `Bytes`.
17. Feeders enqueue deliveries through their selected writer lanes.
18. Lane tasks forward them to the relevant connection writers.
19. Each connection writer pipelines writes across subscribers while preserving
    per-subscriber order.
20. Each client event router has already associated the event stream with its
    subscription ID.
21. Subscription I/O tasks read the shared binary event frame.
22. Dispatch tasks decode the 64 payloads and enqueue them for application
    consumption.
23. `Subscription::next_event` returns them one at a time.

The batch was encoded once on publish ingress and once for event fanout, not
once per subscriber.

## 20. How to study the code

Read in this order and follow each symbol with editor "go to definition":

1. `crates/felix-wire/src/lib.rs`
   - `FrameHeader`
   - `Frame`
   - `Message`
   - binary publish/event encoders
2. `crates/felix-transport/src/lib.rs`
   - `TransportConfig`
   - `QuicServer`
   - `QuicClient`
   - `QuicConnection`
3. `crates/felix-client/src/client/client.rs`
   - `Client::connect_with_transport`
   - `Client::subscribe`
4. `crates/felix-client/src/client/publisher.rs`
   - `Publisher::select_worker`
   - `Publisher::publish_batch_binary`
   - `run_publisher_writer`
5. `services/broker/src/transport/quic/conn.rs`
   - `serve_with_shutdown`
   - `build_publish_context`
   - `handle_connection_with_shutdown`
6. `services/broker/src/transport/quic/streams/handlers.rs`
   - `handle_stream`
7. `services/broker/src/transport/quic/streams/control.rs`
   - `run_control_loop`
8. `services/broker/src/transport/quic/handlers/publish.rs`
   - `resolve_stream_cached`
   - `enqueue_publish`
9. `crates/felix-broker/src/lib.rs`
   - `StreamState`
   - `DeliveryEnvelope`
   - `Broker::publish_batch_to_handle`
   - `Broker::subscribe`
10. `services/broker/src/transport/quic/handlers/subscribe.rs`
    - `handle_subscribe_message`
    - `run_lane_feeder`
    - `run_writer_lane`
    - `run_connection_writer`
11. `crates/felix-client/src/client/event_router.rs`
    - `run_event_router`
12. `crates/felix-client/src/client/subscription.rs`
    - `Subscription::spawn_pipeline`
    - `run_subscription_io_task`
    - `run_subscription_dispatch_task`

After reading, draw the path yourself and annotate every boundary with:

- the task that owns it;
- queue capacity and policy;
- copied versus shared data;
- ordering guarantee;
- acknowledgement meaning;
- failure behavior; and
- relevant metrics.

If you can explain those annotations without reopening the code, you understand
the current Felix data plane.

## Related guides

- [System Design](../architecture/system-design.md)
- [Component Architecture](../architecture/components.md)
- [Wire Protocol](../architecture/wire-protocol.md)
- [Internals: The Publish Path](internals-publish.md)
- [Internals: Subscribe & Fanout](internals-subscribe.md)
- [Internals: Backpressure & Core Sharding](internals-concurrency.md)
- [Graceful Shutdown](../deployment/graceful-shutdown.md)
- [Performance Tuning](../features/performance.md)
