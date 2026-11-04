// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef BRPC_URMA_RESOURCE_POOL_H
#define BRPC_URMA_RESOURCE_POOL_H

#if BRPC_WITH_URMA
#include <atomic>
#include <array>
#include <functional>
#include <map>
#include <memory>
#include <string>
#include <tuple>
#include <vector>
#include "brpc/socket.h"
#include "brpc/urma/urma_memory_pool.h"
#include "butil/synchronization/lock.h"
#include "butil/containers/stack_container.h"
#include "urma_api.h"

#ifndef BRPC_URMA_SINGLE_SGE
#define BRPC_URMA_SINGLE_SGE 0
#endif

namespace brpc {
namespace urma {
DECLARE_bool(urma_jfs_affinity);
DECLARE_int32(urma_io_jfs_cnt);
DECLARE_int32(urma_io_jfr_cnt);
DECLARE_int32(urma_io_sq_depth);
DECLARE_int32(urma_io_rx_depth);
DECLARE_int32(urma_header_buf_count);

// Fixed 64-byte wire header. Multi-byte fields use network byte order.
// CTP limits the complete SEND message, including every SGE, to 4 KiB.
constexpr size_t URMA_CTP_MAX_MESSAGE_SIZE = 4096;
enum UrmaMessageType { MSG_SMALL = 1 };
struct UrmaMsgHeader {
    uint64_t dst_socket_id;
    uint16_t seq;
    uint8_t type;
    uint8_t reserved[53];
} __attribute__((packed));
static_assert(sizeof(UrmaMsgHeader) == 64, "URMA header must be 64 bytes");

class ImportedJfrCache {
public:
    ~ImportedJfrCache();
    urma_target_jetty_t* Acquire(urma_context_t*, urma_rjfr_t*, urma_token_t*);
    void Release(urma_target_jetty_t*);
    size_t size() const;
private:
    // Include UASID: EID + numeric id alone may alias different address spaces.
    typedef std::tuple<std::string, uint32_t, uint32_t> Key;
    struct Entry {
        urma_target_jetty_t* target;
        uint32_t refs;
    };
    mutable butil::Mutex _mutex;
    std::map<Key, Entry> _entries;
};

struct UrmaConnResource {
    std::atomic<bool> failed{false};
    uint32_t local_jfr_index{0};
    urma_target_jetty_t* remote_jfr{nullptr};
    urma_target_seg_t* remote_seg{nullptr};
    std::shared_ptr<ImportedJfrCache> imports;
    ~UrmaConnResource();
};

struct TxBatch;
struct TxCompletionCtx {
    // The submitting path retains one reference through post result handling.
    // A second reference, armed before post, belongs to the expected CQE.
    void AddRef() { refs.fetch_add(1, std::memory_order_relaxed); }
    void Release() {
        if (refs.fetch_sub(1, std::memory_order_acq_rel) == 1) { delete this; }
    }
    std::atomic<uint32_t> refs{1};
    urma_jfs_wr_t wr{};
    butil::StackVector<urma_sge_t, 16> sge;
    SocketId self_socket_id{INVALID_SOCKET_ID};
    uint32_t jfs_index{0};
    UrmaMsgHeader* header_buf{nullptr};
    butil::IOBuf send_data;
    // A disconnected endpoint may disappear before DMA finishes. Keep its
    // imports, as well as its data, alive through the final CQE.
    std::shared_ptr<UrmaConnResource> connection;
    uint16_t seq{0};
    uint8_t type{MSG_SMALL};
    bool header_in_send_data{false};
    size_t body_bytes{0};
    uint32_t registry_index{0};
    TxBatch* batch{nullptr};
    ~TxCompletionCtx();
};

constexpr size_t URMA_TX_BATCH_MAX = 32;
struct TxBatch {
    static TxBatch* Unlinked() { return reinterpret_cast<TxBatch*>(uintptr_t(-1)); }
    void AddRef() { refs.fetch_add(1, std::memory_order_relaxed); }
    void Release() {
        if (refs.fetch_sub(1, std::memory_order_acq_rel) == 1) { delete this; }
    }
    void Append(TxCompletionCtx* ctx) {
        ctx->batch = this;
        AddRef();
        items[count++] = ctx;
    }
    // Only for nodes not published to a JFS; no admitted credits to return.
    void DiscardPrepared() {
        for (size_t i = begin; i < count; ++i) {
            if (items[i]) { items[i]->Release(); items[i] = nullptr; }
        }
        Release();
    }
    std::atomic<TxBatch*> next{Unlinked()};
    std::atomic<uint32_t> refs{1};
    std::array<TxCompletionCtx*, URMA_TX_BATCH_MAX> items{};
    size_t begin{0};
    size_t count{0};
};

// Combined close bit and operation count prevent entry racing shutdown.
class UrmaOperationGate {
public:
    bool Enter() {
        uint64_t old = state.load(std::memory_order_acquire);
        while (!(old & kClosed)) {
            if (state.compare_exchange_weak(old, old + 1,
                    std::memory_order_acq_rel, std::memory_order_acquire)) { return true; }
        }
        return false;
    }
    void Leave() { state.fetch_sub(1, std::memory_order_release); }
    void Close() { state.fetch_or(kClosed, std::memory_order_acq_rel); }
    bool Idle() const { return (state.load(std::memory_order_acquire) & ~kClosed) == 0; }
private:
    static constexpr uint64_t kClosed = uint64_t(1) << 63;
    std::atomic<uint64_t> state{0};
};
class UrmaOperation {
public:
    explicit UrmaOperation(UrmaOperationGate* gate) : _gate(gate->Enter() ? gate : nullptr) {}
    ~UrmaOperation() { if (_gate) { _gate->Leave(); } }
    explicit operator bool() const { return _gate != nullptr; }
    UrmaOperation(const UrmaOperation&) = delete;
    UrmaOperation& operator=(const UrmaOperation&) = delete;
private:
    UrmaOperationGate* _gate;
};
struct RxRecvCtx {
    UrmaMsgHeader* header_buf{nullptr};
    void* data_block{nullptr};
    butil::IOBuf data_iobuf;
    uint32_t jfr_index{0};
    bool posted{false}; // Single CQ consumer owns all RX state after startup.
    ~RxRecvCtx() { FreeHeaderBuffer(header_buf); }
};

// SocketUser's virtual base makes the slot type check safe (no struct punning).
// The pool and the carrier Socket each hold a reference. A queued epoll callback
// can therefore outlive pool shutdown without accessing a freed slot.
struct UrmaIoSlot : public SocketUser {
    enum SlotType { JFS_SLOT, JFR_SLOT };
    explicit UrmaIoSlot(SlotType t, uint32_t i) : type(t), index(i) {}
    virtual ~UrmaIoSlot() = default;
    void AddRef() { refs.fetch_add(1); }
    void Release() { if (refs.fetch_sub(1) == 1) { delete this; } }
    void BeforeRecycle(Socket*) override { Release(); }
    const SlotType type;
    const uint32_t index;
    urma_jfc_t* jfc{nullptr};
    urma_jfce_t* jfce{nullptr};
    SocketId jfc_sid{INVALID_SOCKET_ID};
    UrmaOperationGate operations;
    std::atomic<bool> stopped{false};
private:
    std::atomic<uint32_t> refs{1};
};
class UrmaResourcePool;
struct UrmaJfsSlot;
struct UrmaWriteWaiter {
    UrmaWriteWaiter(UrmaResourcePool*, butil::atomic<int>*, UrmaJfsSlot* = nullptr);
    ~UrmaWriteWaiter();
    UrmaResourcePool* const pool;
    UrmaJfsSlot* const slot;
    butil::atomic<int>* const butex;
    UrmaWriteWaiter* prev{nullptr};
    UrmaWriteWaiter* next{nullptr};
    bool queued{false};
    UrmaWriteWaiter(const UrmaWriteWaiter&) = delete;
    UrmaWriteWaiter& operator=(const UrmaWriteWaiter&) = delete;
};
struct UrmaJfsSlot : public UrmaIoSlot {
    explicit UrmaJfsSlot(uint32_t i) : UrmaIoSlot(JFS_SLOT, i) {}
    urma_jfs_t* jfs{nullptr};
    std::atomic<uint32_t> sq_inflight{0};
    std::atomic<uint32_t> sq_device_inflight{0};
    uint32_t sq_depth{0};
    enum { RUNNING = 1, KICK = 2 };
    std::atomic<TxBatch*> incoming_head{nullptr};
    TxBatch* pending_head{nullptr};
    TxBatch* pending_tail{nullptr};
    std::atomic<unsigned> drain_state{0};
    std::unique_ptr<std::atomic<TxCompletionCtx*>[]> submitted;
    uint32_t next_registry{0};
    // Only waiter registration/cancellation uses this mutex, never post/poll.
    butil::Mutex writers_mutex;
    UrmaWriteWaiter* writers{nullptr};
    std::atomic<bool> has_writers{false};
    // Inspection at quiescent points (tests/debug), never used by CQ routing.
    std::vector<TxCompletionCtx*> SubmittedContexts() const {
        std::vector<TxCompletionCtx*> result;
        for (uint32_t i = 0; submitted && i < sq_depth; ++i) {
            auto* ctx = submitted[i].load(std::memory_order_acquire);
            if (ctx) { result.push_back(ctx); }
        }
        return result;
    }
};
struct UrmaJfrSlot : public UrmaIoSlot {
    explicit UrmaJfrSlot(uint32_t i) : UrmaIoSlot(JFR_SLOT, i) {}
    urma_jfr_t* jfr{nullptr};
    uint32_t rx_depth{0};
    uint32_t jfr_id{0};
    urma_token_t token_value{};
    std::unique_ptr<RxRecvCtx[]> contexts;
    uint32_t rx_posted{0};
    bool Owns(const RxRecvCtx* ctx) const {
        const uintptr_t address = reinterpret_cast<uintptr_t>(ctx);
        const uintptr_t base = reinterpret_cast<uintptr_t>(contexts.get());
        return contexts && address >= base &&
            address - base < sizeof(RxRecvCtx) * rx_depth &&
            (address - base) % sizeof(RxRecvCtx) == 0;
    }
};

// Resource ownership and selection are independent of endpoint parsing. The
// adapter owns event integration; policy subclasses can replace slot selection.
class UrmaResourcePool {
public:
    struct EventAdapter {
        std::function<int(UrmaIoSlot*)> attach;
        std::function<void(UrmaIoSlot*)> detach;
    };
    virtual ~UrmaResourcePool();
    virtual int Initialize(urma_context_t*, const EventAdapter&);
    virtual bool Shutdown();
    virtual UrmaJfsSlot* AcquireJfs();
    UrmaJfsSlot* AcquirePreferredJfs(uint32_t preferred);
    virtual uint32_t PickJfr();
    bool IsWritable(uint32_t preferred = 0) const;
    static uint32_t PreferredJfs(SocketId, size_t count);
    UrmaJfsSlot* PickBackup(uint32_t preferred) const;
    virtual std::array<uint32_t, 4> SampleBackups(uint32_t preferred) const;
    // The KeepWrite stack owns the waiter; only the preferred JFS notifies it.
    int WaitWritable(butil::atomic<int>* butex, int expected,
                     const timespec* deadline, uint32_t preferred = 0);
    // Every returned reservation, including post failures, may unblock peers.
    void ReleaseJfs(UrmaJfsSlot* slot);
    virtual int PostSend(UrmaJfsSlot*, urma_jfs_wr_t*, TxCompletionCtx*,
                         urma_jfs_wr_t** bad = nullptr);
    UrmaJfsSlot* SelectJfs(uint32_t preferred, bool affinity) const;
    int Enqueue(UrmaJfsSlot*, TxBatch*);
    void KickDrain(UrmaJfsSlot*);
    void CompleteSend(UrmaJfsSlot*, const urma_cr_t&);
    UrmaOperationGate operations;
    // Startup only; subsequent RX state is owned by the single CQ consumer.
    virtual int PostRecvToSlot(UrmaJfrSlot&, uint32_t num);
    // Reuse a completed array entry. A failed repost remains owned by the
    // fixed array; successful JFR deletion is its DMA quiescence boundary.
    virtual int RepostRecv(UrmaJfrSlot&, RxRecvCtx*);
    std::vector<UrmaJfsSlot*> io_jfs;
    std::vector<UrmaJfrSlot*> io_jfr;
    std::atomic<uint32_t> jfs_rr_counter{0};
    std::atomic<uint32_t> jfr_rr_counter{0};
    std::shared_ptr<ImportedJfrCache> imports{new ImportedJfrCache};
private:
    void CollectIncoming(UrmaJfsSlot*);
    void DrainSendQueue(UrmaJfsSlot*);
    void CancelQueued(UrmaJfsSlot*);
    friend struct UrmaWriteWaiter;
    using WriteWaiter = UrmaWriteWaiter;
    void RemoveWriterLocked(WriteWaiter* waiter);
    void WakeWritersLocked(UrmaJfsSlot*, bool all = false);
    std::atomic<uint32_t> _active_writers{0};
    std::atomic<bool> _writers_closed{false};
    int CreateCompletionQueue(urma_context_t*, UrmaIoSlot*, uint32_t);
    EventAdapter _events;
};
extern UrmaResourcePool* g_urma_pool;
typedef UrmaResourcePool* (*UrmaResourcePoolFactory)();
// Configure a replacement before global initialization (not thread safe).
bool SetUrmaResourcePoolFactory(UrmaResourcePoolFactory factory);
UrmaResourcePool* CreateUrmaResourcePool();

}  // namespace urma
}  // namespace brpc
#endif
#endif
