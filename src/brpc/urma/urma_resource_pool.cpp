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

#include "brpc/urma/urma_resource_pool.h"
#include "bthread/butex.h"

#if BRPC_WITH_URMA
#include <cerrno>
#include <algorithm>
#include <thread>
#include <mutex>
#include <cstring>
#include <gflags/gflags.h>
#include "butil/fd_utility.h"
#include "butil/scoped_lock.h"
#include "brpc/urma/urma_helper.h"

namespace brpc {
namespace urma {
DEFINE_bool(urma_jfs_affinity, true, "Prefer a stable shared JFS for each connection");
DEFINE_int32(urma_io_jfs_cnt, 48, "Number of process-wide IO JFS slots");
DEFINE_int32(urma_io_jfr_cnt, 64, "Number of process-wide shared IO JFR slots");
DEFINE_int32(urma_io_sq_depth, 128, "Depth of each pooled IO send queue");
DEFINE_int32(urma_io_rx_depth, 512, "Depth of each shared IO receive queue");
UrmaResourcePool* g_urma_pool = nullptr;
TxCompletionCtx::~TxCompletionCtx() {
    if (!header_in_send_data) { FreeHeaderBuffer(header_buf); }
    if (batch) { batch->Release(); }
}
static UrmaResourcePoolFactory g_resource_factory = nullptr;
bool SetUrmaResourcePoolFactory(UrmaResourcePoolFactory factory) {
    if (g_urma_pool) { errno = EBUSY; return false; }
    g_resource_factory = factory;
    return true;
}
UrmaResourcePool* CreateUrmaResourcePool() {
    return g_resource_factory ? g_resource_factory() : new UrmaResourcePool;
}

urma_target_jetty_t* ImportedJfrCache::Acquire(
        urma_context_t* context, urma_rjfr_t* remote, urma_token_t* token) {
    const Key key(std::string(reinterpret_cast<const char*>(remote->jfr_id.eid.raw), 16),
                  remote->jfr_id.uasid, remote->jfr_id.id);
    BAIDU_SCOPED_LOCK(_mutex);
    auto it = _entries.find(key);
    if (it != _entries.end()) {
        ++it->second.refs;
        return it->second.target;
    }
    urma_target_jetty_t* target = urma_import_jfr(context, remote, token);
    if (target) { _entries.emplace(key, Entry{target, 1}); }
    return target;
}
void ImportedJfrCache::Release(urma_target_jetty_t* target) {
    BAIDU_SCOPED_LOCK(_mutex);
    for (auto it = _entries.begin(); it != _entries.end(); ++it) {
        if (it->second.target != target) { continue; }
        CHECK_GT(it->second.refs, 0u);
        if (--it->second.refs == 0) {
            // Keep a zero-ref entry on provider EAGAIN, so it can be reused or
            // retried at shutdown instead of losing the kernel handle.
            if (urma_unimport_jfr(target) == URMA_SUCCESS) {
                _entries.erase(it);
            }
        }
        return;
    }
    CHECK(false) << "Unknown imported URMA JFR";
}
ImportedJfrCache::~ImportedJfrCache() {
    for (const auto& item : _entries) {
        CHECK_EQ(item.second.refs, 0u);
        if (urma_unimport_jfr(item.second.target) != URMA_SUCCESS) {
            LOG(ERROR) << "Failed to release imported URMA JFR";
        }
    }
}
size_t ImportedJfrCache::size() const {
    BAIDU_SCOPED_LOCK(_mutex);
    return _entries.size();
}
UrmaConnResource::~UrmaConnResource() {
    if (remote_jfr) { imports->Release(remote_jfr); }
    if (remote_seg && urma_unimport_seg(remote_seg) != URMA_SUCCESS) {
        LOG(ERROR) << "Failed to unimport URMA segment";
    }
}

int UrmaResourcePool::CreateCompletionQueue(
        urma_context_t* context, UrmaIoSlot* slot, uint32_t depth) {
    slot->jfce = urma_create_jfce(context);
    if (!slot->jfce || slot->jfce->fd < 0 ||
        butil::make_non_blocking(slot->jfce->fd) != 0) { return -1; }
    urma_jfc_cfg_t cfg{};
    cfg.depth = depth;
    cfg.jfce = slot->jfce;
    slot->jfc = urma_create_jfc(context, &cfg);
    return slot->jfc ? 0 : -1;
}
int UrmaResourcePool::Initialize(urma_context_t* context,
                                 const EventAdapter& events) {
    if (!context || !io_jfs.empty() || !io_jfr.empty() ||
        FLAGS_urma_io_jfs_cnt <= 0 || FLAGS_urma_io_jfr_cnt <= 0 ||
        FLAGS_urma_io_sq_depth <= 0 || FLAGS_urma_io_rx_depth <= 0 ||
        FLAGS_urma_io_sq_depth > 65535 || FLAGS_urma_io_rx_depth > 65535) {
        errno = EINVAL;
        return -1;
    }
    _events = events;
    for (int i = 0; i < FLAGS_urma_io_jfr_cnt; ++i) {
        auto* slot = new UrmaJfrSlot(i);
        io_jfr.push_back(slot);
        slot->rx_depth = FLAGS_urma_io_rx_depth;
        if (CreateCompletionQueue(context, slot, slot->rx_depth) != 0) { return -1; }
        urma_jfr_cfg_t cfg{};
        cfg.depth = slot->rx_depth;
        cfg.trans_mode = URMA_TM_RM;
        cfg.max_sge = 2;
        cfg.min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
        cfg.flag.bs.token_policy = URMA_TOKEN_NONE;
        cfg.jfc = slot->jfc;
        slot->jfr = urma_create_jfr(context, &cfg);
        if (!slot->jfr) { return -1; }
        slot->jfr_id = slot->jfr->jfr_id.id;
        if (PostRecvToSlot(*slot, slot->rx_depth) != 0 ||
            urma_rearm_jfc(slot->jfc, false) != URMA_SUCCESS ||
            (_events.attach && _events.attach(slot) != 0)) { return -1; }
    }
    for (int i = 0; i < FLAGS_urma_io_jfs_cnt; ++i) {
        auto* slot = new UrmaJfsSlot(i);
        io_jfs.push_back(slot);
        slot->sq_depth = FLAGS_urma_io_sq_depth;
        slot->submitted.reset(new std::atomic<TxCompletionCtx*>[slot->sq_depth]);
        for (uint32_t j = 0; j < slot->sq_depth; ++j) {
            slot->submitted[j].store(nullptr, std::memory_order_relaxed);
        }
        if (CreateCompletionQueue(context, slot, slot->sq_depth) != 0) { return -1; }
        urma_jfs_cfg_t cfg{};
        cfg.depth = slot->sq_depth;
        cfg.trans_mode = URMA_TM_RM;
        cfg.priority = GetUrmaJettyPriority();
        cfg.max_sge = BRPC_URMA_SINGLE_SGE ? 1 : GetUrmaMaxSge();
        cfg.rnr_retry = URMA_TYPICAL_RNR_RETRY;
        cfg.err_timeout = URMA_TYPICAL_ERR_TIMEOUT;
        cfg.jfc = slot->jfc;
        slot->jfs = urma_create_jfs(context, &cfg);
        if (!slot->jfs || urma_rearm_jfc(slot->jfc, false) != URMA_SUCCESS ||
            (_events.attach && _events.attach(slot) != 0)) { return -1; }
    }
    return 0;
}
UrmaJfsSlot* UrmaResourcePool::AcquireJfs() {
    if (io_jfs.empty()) { errno = ENODEV; return nullptr; }
    const uint32_t start = jfs_rr_counter.fetch_add(1, std::memory_order_relaxed);
    for (size_t i = 0; i < io_jfs.size(); ++i) {
        UrmaJfsSlot* slot = io_jfs[(start + i) % io_jfs.size()];
        if (slot->stopped.load()) { continue; }
        uint32_t used = slot->sq_inflight.load(std::memory_order_relaxed);
        while (used < slot->sq_depth) {
            if (slot->sq_inflight.compare_exchange_weak(used, used + 1)) {
                return slot;
            }
        }
    }
    errno = EAGAIN;
    return nullptr;
}
UrmaJfsSlot* UrmaResourcePool::AcquirePreferredJfs(uint32_t preferred) {
    if (io_jfs.empty()) { errno = ENODEV; return nullptr; }
    // Prefer affinity, but preserve pool-wide backpressure/wakeup semantics.
    for (size_t i = 0; i < io_jfs.size(); ++i) {
        auto* slot = io_jfs[(preferred + i) % io_jfs.size()];
        if (slot->stopped.load()) { continue; }
        uint32_t n = slot->sq_inflight.load(std::memory_order_relaxed);
        while (n < slot->sq_depth) {
            if (slot->sq_inflight.compare_exchange_weak(n, n + 1)) return slot;
        }
    }
    errno = EAGAIN;
    return nullptr;
}
uint32_t UrmaResourcePool::PickJfr() {
    return jfr_rr_counter.fetch_add(1, std::memory_order_relaxed) % io_jfr.size();
}
bool UrmaResourcePool::IsWritable() const {
    for (const auto* slot : io_jfs) {
        if (!slot->stopped.load() && slot->sq_inflight.load() < slot->sq_depth) {
            return true;
        }
    }
    return false;
}
UrmaResourcePool::WriteWaiter::~WriteWaiter() {
    const int saved_errno = errno;
    {
        BAIDU_SCOPED_LOCK(pool->_writers_mutex);
        pool->RemoveWriterLocked(this);
    }
    // Last access to the pool: Shutdown waits for returning guards as well
    // as queued waiters before allowing the pool mutex to be destroyed.
    pool->_active_writers.fetch_sub(1);
    errno = saved_errno;
}

void UrmaResourcePool::RemoveWriterLocked(WriteWaiter* waiter) {
    if (!waiter->queued) { return; }
    if (waiter->prev) { waiter->prev->next = waiter->next; }
    else { _writers = waiter->next; }
    if (waiter->next) { waiter->next->prev = waiter->prev; }
    waiter->queued = false;
    _has_writers.store(_writers != nullptr);
}

void UrmaResourcePool::WakeWritersLocked() {
    while (_writers) {
        auto* waiter = _writers;
        RemoveWriterLocked(waiter);
        // Cancellation must acquire _writers_mutex before the stack node or
        // its Socket butex can disappear. Do not retain either after unlocking.
        waiter->butex->fetch_add(1, butil::memory_order_release);
        bthread::butex_wake_except(waiter->butex, INVALID_BTHREAD);
    }
}

int UrmaResourcePool::WaitWritable(butil::atomic<int>* butex, int expected,
                                    const timespec* deadline) {
    WriteWaiter waiter(this, butex);
    {
        BAIDU_SCOPED_LOCK(_writers_mutex);
        if (_writers_closed) { errno = EIO; return -1; }
        waiter.next = _writers;
        if (_writers) { _writers->prev = &waiter; }
        _writers = &waiter;
        waiter.queued = true;
        // Publish BEFORE checking space. Together with the sequentially
        // consistent credit decrement/check in ReleaseJfs, this prevents both
        // sides missing each other when completion races registration.
        _has_writers.store(true);
        if (IsWritable()) {
            RemoveWriterLocked(&waiter);
            return 0;
        }
    }
    // The caller sampled expected before checking writability. A notification
    // before this wait changes the butex value and produces EWOULDBLOCK.
    // The stack guard unregisters before returning and preserves errno.
    return bthread::butex_wait(butex, expected, deadline);
}

void UrmaResourcePool::ReleaseJfs(UrmaJfsSlot* slot) {
    slot->sq_inflight.fetch_sub(1);
    // No allocations, mutex acquisitions or connection scans without waiters.
    if (!_has_writers.load()) { return; }
    BAIDU_SCOPED_LOCK(_writers_mutex);
    WakeWritersLocked();
}

UrmaJfsSlot* UrmaResourcePool::SelectJfs(uint32_t preferred, bool affinity) const {
    if (io_jfs.empty()) { errno = ENODEV; return nullptr; }
    (void)affinity;
    for (size_t i = 0; i < io_jfs.size(); ++i) {
        auto* slot = io_jfs[(preferred + i) % io_jfs.size()];
        if (!slot->stopped.load() && slot->sq_inflight.load() < slot->sq_depth) { return slot; }
    }
    errno = EAGAIN;
    return nullptr;
}

int UrmaResourcePool::PostSend(UrmaJfsSlot* slot, urma_jfs_wr_t* wr,
                               TxCompletionCtx*, urma_jfs_wr_t** bad) {
    urma_jfs_wr_t* first_bad = nullptr;
    const urma_status_t rc = urma_post_jfs_wr(slot->jfs, wr, &first_bad);
    if (bad) { *bad = first_bad; }
    if (rc != URMA_SUCCESS) { errno = rc > 0 ? rc : EIO; }
    return rc == URMA_SUCCESS ? 0 : -1;
}

int UrmaResourcePool::Enqueue(UrmaJfsSlot* slot, TxBatch* batch) {
    UrmaOperation active(&slot->operations);
    if (!active || slot->stopped.load()) { errno = EIO; return -1; }
    slot->sq_inflight.fetch_add(batch->count, std::memory_order_acq_rel);
    TxBatch* previous = slot->incoming_head.exchange(batch, std::memory_order_acq_rel);
    batch->next.store(previous, std::memory_order_release);
    KickDrain(slot);
    return 0;
}

void UrmaResourcePool::CollectIncoming(UrmaJfsSlot* slot) {
    TxBatch* node = slot->incoming_head.exchange(nullptr, std::memory_order_acq_rel);
    if (!node) { return; }
    TxBatch* newest = node;
    TxBatch* fifo = nullptr;
    while (node) {
        TxBatch* next;
        while ((next = node->next.load(std::memory_order_acquire)) == TxBatch::Unlinked()) {
            std::this_thread::yield();
        }
        node->next.store(fifo, std::memory_order_relaxed);
        fifo = node;
        node = next;
    }
    if (slot->pending_tail) { slot->pending_tail->next.store(fifo, std::memory_order_relaxed); }
    else { slot->pending_head = fifo; }
    slot->pending_tail = newest;
}

namespace {
void FailTxContext(TxCompletionCtx* ctx, int error, const char* reason) {
    if (ctx->connection) { ctx->connection->failed.store(true, std::memory_order_release); }
    SocketUniquePtr socket;
    if (Socket::Address(ctx->self_socket_id, &socket) == 0) {
        socket->SetFailed(error, "%s", reason);
    }
}
void RemoveQueueFront(UrmaJfsSlot* slot) {
    TxBatch* node = slot->pending_head;
    node->items[node->begin++] = nullptr;
    if (node->begin == node->count) {
        slot->pending_head = node->next.load(std::memory_order_relaxed);
        if (!slot->pending_head) { slot->pending_tail = nullptr; }
        node->Release();
    }
}
}

void UrmaResourcePool::CancelQueued(UrmaJfsSlot* slot) {
    CollectIncoming(slot);
    while (slot->pending_head) {
        auto* ctx = slot->pending_head->items[slot->pending_head->begin];
        FailTxContext(ctx, EIO, "URMA send queue closed");
        RemoveQueueFront(slot);
        slot->sq_inflight.fetch_sub(1, std::memory_order_acq_rel);
        ctx->Release();
    }
}

void UrmaResourcePool::DrainSendQueue(UrmaJfsSlot* slot) {
    for (;;) {
        if (slot->stopped.load()) { CancelQueued(slot); return; }
        if (!slot->pending_head) { CollectIncoming(slot); }
        if (!slot->pending_head) { return; }
        auto* first = slot->pending_head->items[slot->pending_head->begin];
        if (first->connection && first->connection->failed.load(std::memory_order_acquire)) {
            RemoveQueueFront(slot);
            slot->sq_inflight.fetch_sub(1, std::memory_order_acq_rel);
            first->Release();
            continue;
        }
        uint32_t used = slot->sq_device_inflight.load(std::memory_order_acquire);
        if (used >= slot->sq_depth) { return; }
        // Include already-published nodes without waiting for a fuller batch.
        CollectIncoming(slot);
        const size_t limit = std::min<size_t>(URMA_TX_BATCH_MAX, slot->sq_depth - used);
        TxCompletionCtx* entries[URMA_TX_BATCH_MAX];
        size_t count = 0;
        for (TxBatch* node = slot->pending_head; node && count < limit;
             node = node->next.load(std::memory_order_relaxed)) {
            for (size_t i = node->begin; i < node->count && count < limit; ++i) {
                auto* ctx = node->items[i];
                if (ctx->connection && ctx->connection->failed.load(std::memory_order_acquire)) {
                    break;
                }
                entries[count++] = ctx;
            }
            if (count && entries[count - 1] != node->items[node->count - 1]) { break; }
        }
        if (!count) { continue; }
        for (size_t i = 0; i < count; ++i) {
            auto* ctx = entries[i];
            ctx->wr.next = i + 1 < count ? &entries[i + 1]->wr : nullptr;
            ctx->AddRef(); // Expected CQE; queue retains its own reference.
            while (slot->submitted[slot->next_registry].load(std::memory_order_acquire)) {
                slot->next_registry = (slot->next_registry + 1) % slot->sq_depth;
            }
            ctx->registry_index = slot->next_registry;
            slot->submitted[slot->next_registry].store(ctx, std::memory_order_release);
            slot->next_registry = (slot->next_registry + 1) % slot->sq_depth;
        }
        slot->sq_device_inflight.fetch_add(count, std::memory_order_acq_rel);
        urma_jfs_wr_t* bad = nullptr;
        const int rc = PostSend(slot, &entries[0]->wr, entries[0], &bad);
        const int error = errno;
        VLOG(1) << "URMA JFS submit index=" << slot->index
                << " chain_wr=" << count << " result=" << rc;
        size_t accepted = count;
        if (rc != 0) {
            accepted = 0;
            while (accepted < count && &entries[accepted]->wr != bad) { ++accepted; }
            // Contract violations cannot safely be treated as an unposted chain.
            CHECK_LT(accepted, count) << "URMA post error without valid bad_wr";
        }
        for (size_t i = accepted; i < count; ++i) {
            auto* ctx = entries[i];
            slot->submitted[ctx->registry_index].store(nullptr, std::memory_order_release);
            ctx->Release(); // Unaccepted WR cannot produce a CQE.
        }
        slot->sq_device_inflight.fetch_sub(count - accepted, std::memory_order_acq_rel);
        for (size_t i = 0; i < accepted; ++i) {
            RemoveQueueFront(slot);
            entries[i]->Release(); // Resolve submission, possibly last reference.
        }
        if (rc != 0) {
            const bool transient = error == URMA_ENOMEM || error == URMA_EAGAIN;
            if (transient && slot->sq_device_inflight.load(std::memory_order_acquire) != 0) {
                return; // A future CQE will retry the retained suffix.
            }
            // No future CQE exists to repair a provider/credit disagreement.
            FailTxContext(entries[accepted], error,
                          transient ? "URMA post stalled without in-flight WR" : "URMA post failed");
            if (!transient && error != URMA_EINVAL) { slot->stopped.store(true); }
        }
    }
}

void UrmaResourcePool::KickDrain(UrmaJfsSlot* slot) {
    UrmaOperation active(&slot->operations);
    if (!active) { return; }
    const unsigned previous = slot->drain_state.fetch_or(
        UrmaJfsSlot::RUNNING | UrmaJfsSlot::KICK, std::memory_order_acq_rel);
    if (previous & UrmaJfsSlot::RUNNING) { return; }
    for (;;) {
        slot->drain_state.fetch_and(~unsigned(UrmaJfsSlot::KICK), std::memory_order_acq_rel);
        DrainSendQueue(slot);
        if (_has_writers.load() && IsWritable()) {
            BAIDU_SCOPED_LOCK(_writers_mutex);
            WakeWritersLocked();
        }
        unsigned expected = UrmaJfsSlot::RUNNING;
        if (slot->drain_state.compare_exchange_strong(expected, 0,
                std::memory_order_acq_rel, std::memory_order_acquire)) { return; }
    }
}

void UrmaResourcePool::CompleteSend(UrmaJfsSlot* slot, const urma_cr_t& cr) {
    auto* ctx = reinterpret_cast<TxCompletionCtx*>(cr.user_ctx);
    slot->submitted[ctx->registry_index].store(nullptr, std::memory_order_release);
    slot->sq_device_inflight.fetch_sub(1, std::memory_order_acq_rel);
    slot->sq_inflight.fetch_sub(1, std::memory_order_acq_rel);
    if (cr.status != URMA_CR_SUCCESS) {
        FailTxContext(ctx, EIO, "URMA send completion failed");
    }
    ctx->Release();
    KickDrain(slot);
}

int UrmaResourcePool::PostRecvToSlot(UrmaJfrSlot& slot, uint32_t num) {
    for (uint32_t i = 0; i < num; ++i) {
        std::unique_ptr<RxRecvCtx> context(new RxRecvCtx);
        context->jfr_index = slot.index;
        context->header_buf = static_cast<UrmaMsgHeader*>(AllocHeaderBuffer());
        if (!context->header_buf) { errno = ENOMEM; return -1; }
        std::memset(context->header_buf, 0, sizeof(UrmaMsgHeader));
        slot.pending.insert(context.get());
        if (RepostRecv(slot, context.get()) != 0) {
            slot.pending.erase(context.get());
            return -1;
        }
        context.release();
    }
    return 0;
}

int UrmaResourcePool::RepostRecv(UrmaJfrSlot& slot, RxRecvCtx* context) {
    const size_t block_size = GetUrmaRecvBlockSize();
    if (!context->data_block) {
        // Finish the stream before the WR becomes visible to the provider.
        int size = 0;
        bool allocated = false;
        {
            butil::IOBufAsZeroCopyOutputStream stream(
                &context->data_iobuf, block_size + sizeof(butil::IOBuf::Block));
            allocated = stream.Next(&context->data_block, &size);
        }
        if (!allocated || size < static_cast<int>(block_size)) {
            context->data_block = nullptr;
            context->data_iobuf.clear();
            errno = ENOMEM;
            return -1;
        }
    }
    urma_sge_t sge[2]{};
    sge[0].addr = reinterpret_cast<uint64_t>(context->header_buf);
    sge[0].len = sizeof(UrmaMsgHeader);
    sge[0].tseg = GetHeaderPoolSegFor(context->header_buf);
    sge[1].addr = reinterpret_cast<uint64_t>(context->data_block);
    sge[1].len = block_size;
    sge[1].tseg = GetPoolSegFor(context->data_block);
    if (!sge[0].tseg || !sge[1].tseg) { errno = ERDMAMEM; return -1; }
    urma_jfr_wr_t wr{};
    wr.src = urma_sg_t{sge, 2};
    wr.user_ctx = reinterpret_cast<uint64_t>(context);
    urma_jfr_wr_t* bad = nullptr;
    if (urma_post_jfr_wr(slot.jfr, &wr, &bad) != URMA_SUCCESS) {
        errno = EIO;
        return -1;
    }
    return 0;
}

bool UrmaResourcePool::Shutdown() {
    operations.Close();
    {
        BAIDU_SCOPED_LOCK(_writers_mutex);
        _writers_closed = true;
        WakeWritersLocked();
    }
    while (_active_writers.load() != 0 || !operations.Idle()) { std::this_thread::yield(); }
    for (auto* slot : io_jfs) {
        slot->operations.Close();
        slot->stopped.store(true);
        if (_events.detach) { _events.detach(slot); }
    }
    for (auto* slot : io_jfr) {
        slot->operations.Close();
        slot->stopped.store(true);
        if (_events.detach) { _events.detach(slot); }
    }
    for (auto* slot : io_jfs) {
        while (!slot->operations.Idle()) { std::this_thread::yield(); }
    }
    for (auto* slot : io_jfr) {
        while (!slot->operations.Idle()) { std::this_thread::yield(); }
    }
    bool success = true;
    for (auto* slot : io_jfs) {
        // Successful deletion is the DMA quiescence boundary. Retain all
        // storage if the provider cannot stop this queue.
        if (slot->jfs && urma_delete_jfs(slot->jfs) != URMA_SUCCESS) {
            success = false;
            continue;
        }
        slot->jfs = nullptr;
        CancelQueued(slot);
        for (uint32_t i = 0; slot->submitted && i < slot->sq_depth; ++i) {
            auto* ctx = slot->submitted[i].exchange(nullptr, std::memory_order_acq_rel);
            if (ctx) { ctx->Release(); }
        }
        slot->sq_inflight.store(0);
        slot->sq_device_inflight.store(0);
    }
    for (auto* slot : io_jfr) {
        BAIDU_SCOPED_LOCK(slot->mutex);
        if (slot->jfr && urma_delete_jfr(slot->jfr) != URMA_SUCCESS) {
            success = false;
            continue;
        }
        slot->jfr = nullptr;
        for (auto* ctx : slot->pending) { delete ctx; }
        slot->pending.clear();
    }
    if (!success) { return false; }
    std::vector<UrmaIoSlot*> slots(io_jfs.begin(), io_jfs.end());
    slots.insert(slots.end(), io_jfr.begin(), io_jfr.end());
    for (auto* slot : slots) {
        std::unique_lock<butil::Mutex> rx_lock(slot->mutex, std::defer_lock);
        if (slot->type == UrmaIoSlot::JFR_SLOT) { rx_lock.lock(); }
        // Consume and acknowledge pending events before deleting the JFC.
        if (slot->jfce && slot->jfc) {
            urma_jfc_t* event = nullptr;
            while (urma_wait_jfc(slot->jfce, 1, 0, &event) > 0) {
                uint32_t count = 1;
                urma_ack_jfc(&event, &count, 1);
            }
        }
        if (slot->jfc && urma_delete_jfc(slot->jfc) != URMA_SUCCESS) {
            success = false;
            continue;
        }
        slot->jfc = nullptr;
        if (slot->jfce && urma_delete_jfce(slot->jfce) != URMA_SUCCESS) {
            success = false;
            continue;
        }
        slot->jfce = nullptr;
    }
    return success;
}
UrmaResourcePool::~UrmaResourcePool() {
    if (!Shutdown()) {
        LOG(ERROR) << "Retain URMA slots because provider cleanup failed";
        return;
    }
    for (auto* slot : io_jfs) { slot->Release(); }
    for (auto* slot : io_jfr) { slot->Release(); }
}
}  // namespace urma
}  // namespace brpc
#endif
