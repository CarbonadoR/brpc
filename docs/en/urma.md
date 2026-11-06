# URMA shared-queue transport

UrmaTransport carries brpc messages through the URMA API in openEuler
[UMDK](https://atomgit.com/openeuler/umdk). The data path uses reliable message
mode (`URMA_TM_RM`), CTP, and separate JFS/JFR objects shared by logical connections.

## Resource organization

`UrmaMemoryPool` owns registered storage, `UrmaResourcePool` owns queues and their
lifecycle, and `UrmaEndpoint` maintains connection state, fragmentation, reordering
and delivery. Memory and resource pool factories and an event adapter separate
these responsibilities.

The process creates 48 JFS send queues and 64 JFR receive queues by default. Each
slot has its own JFC, JFCE and event-carrier Socket. Logical connections retain a
local JFR assignment, peer resource references, versioned SocketIds, sequence
numbers and reorder state rather than allocating dedicated hardware queues.

Imported peer JFRs are cached by EID, UASID and JFR ID with reference counting.
Outstanding sends retain the connection resources even after the endpoint closes.

## Registered memory

The default allocator registers an mmap region and serves fixed-size blocks from
64 independently locked free lists. Allocation starts at a thread-selected shard
and searches other shards when necessary. Data blocks default to 8KiB, including
the IOBuf block header. A separate pool supplies raw 64B wire headers without
routing them through the IOBuf block allocator.

Pools do not automatically grow after initialization. Exhaustion returns failure.
DMA must finish before unregistration. Live IOBuf references require retaining
the mapping and deallocator even if the data segment has already been unregistered.

`UrmaMemoryPool` exposes allocation, deallocation, address ownership, segment
lookup and unregistration. Before global initialization, applications can replace
the allocator through `SetUrmaMemoryPoolFactory`; global initialization then
creates and installs both pools. Replacements must preserve registration and
lifetime guarantees. User storage is managed through `RegisterMemoryForUrma` and
`DeregisterMemoryForUrma`; it must remain registered while sends reference it.

## Connection establishment

A connection negotiates over TCP before switching to URMA. The handshake carries
receive capacity, queue information, the versioned SocketId, EID/UASID, transport
path type and segment metadata. Binary v2 and Protobuf v3 encodings are supported;
the client defaults to v2. The shared-queue protocol requires implementation
version 2 and validates the peer resource fields and address range.

`URMA_ON` is published only after negotiation and resource import complete.
Legacy handshakes without the shared-queue fields cannot establish a pooled
connection. Peers without URMA support follow the existing negotiation failure
and TCP fallback path. Concurrent connections use separate handshake objects so
pending callbacks are not shared.

## Sending

Every SEND fragment carries a 64B header containing the destination SocketId,
a 16-bit sequence number and a message type. Multi-byte fields use network byte
order. CTP limits the complete message to 4096B, leaving at most 4032B for the
body, further constrained by both peers' receive capacity.

The default mode uses one header SGE and one or more body SGEs. Gathering crosses
input IOBufs and backing blocks until the total SGE limit or body limit is reached.
Registered body storage is referenced directly; the entire payload is not copied
just to combine fragments. Unregistered fragments cannot be used directly as DMA
sources.

With `URMA_SINGLE_SGE`, the sender constructs the header in a registered data block
and copies the body immediately after it, submitting one SGE. Input fragmentation
does not consume additional SGEs. Both modes use the same wire format; receiving
always uses separate header and body SGEs.

JFS affinity is enabled by default. A connection initially chooses its preferred
JFS through round-robin assignment, reuses it when possible, and tries other
shared queues when it is full. Each WR reserves an SQ slot. Failed submissions
return it; successful submissions release it on TX completion. The implementation
posts individual WRs rather than submitting WR chains.

A TX context retains the header, data and connection references until completion.
Caller data is consumed only after a successful post. Writers waiting for shared
SQ capacity register with the pool, so space released on any queue can notify
them; registration, notification and shutdown races are coordinated.

## Receiving and delivery

Each JFR has preposted receive contexts. Completion processing validates the
header and length, routes through the destination SocketId, and restores each
connection's ordered byte stream. In-order fragments bypass the reorder map;
out-of-order fragments remain buffered until the gap closes. Sequence arithmetic
supports 16-bit wraparound. Duplicate or invalid fragments fail the connection.

Reposting reuses the receive context and header. Delivered bodies remain alive
through IOBuf references. The handoff uses movable ownership transfers to avoid
extra shallow copies and reference bookkeeping; it does not copy payload bytes.

By default, one CQ poll collects connections that need parsing and invokes the
parser once for each after processing that group of completions. This combines
parser invocations, preserving protocol message boundaries and per-connection
byte order.

Last-message handling defers at most one inline message across the delivery
scope until CQ event ownership has been handed off and relevant locks released.
Other messages use normal scheduling. Business callbacks therefore do not execute
while holding a shared queue lock.

## Concurrency and shutdown

A slot mutex coordinates post, poll and teardown for that queue without
serializing different slots. Pending context sets track unreclaimed work. The
pool and event-carrier Socket both retain slot references, allowing queued event
callbacks to outlive shutdown safely.

Shutdown stops event and queue activity, wakes waiting writers, and releases
queues, completion contexts and imports in dependency order. Unregistration
requires completed DMA; releasing storage additionally requires that application
references no longer use it. Cleanup must not invalidate storage still accessed
by the device or application.

## Building and enabling

UMDK headers and `liburma` are required. CMake enables the module with
`WITH_URMA=ON`, preferring installed SDK headers and optionally downloading a
pinned UMDK revision when headers are absent. `DOWNLOAD_URMA_HEADERS=OFF` disables
that download. SDK locations can be supplied through `URMA_ROOT`,
`URMA_INCLUDE_PATH`, `URMA_BOND_INCLUDE_PATH` and `URMA_LIB`.

Make uses `config_brpc.sh --with-urma` with dependency paths provided through
`--headers` and `--libs`. Bazel uses `BRPC_WITH_URMA=true` and links `liburma`.
Disabling `BRPC_DOWNLOAD_URMA_HEADERS` requires headers declared through the
toolchain or a local dependency.

CMake's `URMA_SINGLE_SGE` and Bazel's `BRPC_URMA_SINGLE_SGE` choose the send mode;
both default to disabled. The setting must apply to the brpc library, not only
the caller.

Enable transport through `ChannelOptions::use_urma` and `ServerOptions::use_urma`.
The current implementation supports `baidu_std`, non-bonding devices and event
mode. Busy polling, end-to-end credit/ACK flow control and one-sided READ/WRITE
for large messages are not implemented. Larger payloads continue to use fragmented
SEND operations.

## Main runtime parameters

| Parameter | Default | Meaning |
| --- | --- | --- |
| `urma_device` | empty | Device name; empty selects the first enumerated device |
| `urma_io_jfs_cnt` | 48 | Process-wide send queue count |
| `urma_io_jfr_cnt` | 64 | Process-wide receive queue count |
| `urma_io_sq_depth` | 128 | In-flight slots per send queue |
| `urma_io_rx_depth` | 512 | Preposted receives per receive queue |
| `urma_buffer_size` | 8192 | Data block size including the IOBuf block header |
| `urma_max_sge` | 0 | Device capability when zero; positive values cap send SGEs |
| `urma_cqe_poll_once` | 32 | Maximum completions per poll |
| `urma_jfs_affinity` | true | Prefer the connection's assigned JFS |
| `urma_rx_batch_parse` | true | Combine parsing per connection within a poll |
| `urma_inline_last` | true | Execute the deferred last message after event handoff |
| `urma_client_handshake_version` | 2 | Client handshake encoding, 2 or 3 |

Configure queue counts, depths and pool parameters before global initialization.
Preposted receives require sufficient data blocks, and headers must cover receive
and send slots. Initialization checks the combination; device capabilities also
constrain queue depths, SGE counts and message size.
