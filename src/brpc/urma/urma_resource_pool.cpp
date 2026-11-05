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
DEFINE_bool(urma_io_jfs_lock_free, false,
            "Disable UMDK internal locking for pooled JFS; the queue owner serializes posts");
DEFINE_bool(urma_io_jfr_lock_free, false,
            "Disable UMDK internal locking for pooled JFR; each JFR has one reposting executor");
DEFINE_bool(urma_io_jfc_lock_free, false,
            "Disable UMDK internal locking for pooled JFC; each JFC has one polling executor");
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
    cfg.flag.bs.lock_free = FLAGS_urma_io_jfc_lock_free;
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
        cfg.flag.bs.lock_free = FLAGS_urma_io_jfr_lock_free;
        cfg.rnr_retry = URMA_TYPICAL_RNR_RETRY;
        cfg.err_timeout = URMA_TYPICAL_ERR_TIMEOUT;
        cfg.jfc = slot->jfc;
        slot->jfs = urma_create_jfs(context, &cfg);
        if (!slot->jfs || urma_rearm_jfc(slot->jfc, false) != URMA_SUCCESS ||
            (_events.attach && _events.attach(slot) != 0)) { return -1; }
    }
    return 0;
}
uint32_t UrmaResourcePool::PickJfr() {
    return jfr_rr_counter.fetch_add(1, std::memory_order_relaxed) % io_jfr.size();
}
namespace {
uint64_t Mix64(uint64_t value) {
    value = (value ^ (value >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    value = (value ^ (value >> 27)) * UINT64_C(0x94d049bb133111eb);
    return value ^ (value >> 31);
}
uint64_t RotateLeft(uint64_t value, unsigned bits) {
    return (value << bits) | (value >> (64 - bits));
}
struct JfsRandom {
    JfsRandom() {
        cfg.flag.bs.lock_free = FLAGS_urma_io_jfs_lock_free;
        static std::atomic<uint64_t> sequence{1};
        uint64_t seed = sequence.fetch_add(1, std::memory_order_relaxed) ^
            std::hash<std::thread::id>()(std::this_thread::get_id());
        for (auto& item : state) {
            seed += UINT64_C(0x9e3779b97f4a7c15);
            item = Mix64(seed);
        }
    }
    uint64_t Next() { // xoshiro256++; each pthread owns its state.
        const uint64_t result = RotateLeft(state[0] + state[3], 23) + state[0];
        const uint64_t t = state[1] << 17;
        state[2] ^= state[0]; state[3] ^= state[1];
        state[1] ^= state[2]; state[0] ^= state[3];
        state[2] ^= t; state[3] = RotateLeft(state[3], 45);
        return result;
    }
    uint64_t state[4];
};
}
uint32_t UrmaResourcePool::PreferredJfs(SocketId sid, size_t count) {
    return count ? Mix64(sid) % count : 0;
}
std::array<uint32_t, 4> UrmaResourcePool::SampleBackups(uint32_t preferred) const {
    std::array<uint32_t, 4> candidates{};
    preferred %= io_jfs.size();
    const size_t count = std::min<size_t>(4, io_jfs.size() - 1);
    if (io_jfs.size() <= 5) {
        for (size_t i = 0; i < count; ++i) {
            candidates[i] = (preferred + 1 + i) % io_jfs.size();
        }
    } else {
        thread_local JfsRandom random;
        for (size_t i = 0; i < count;) {
            const uint32_t candidate = random.Next() % io_jfs.size();
            if (candidate == preferred ||
                std::find(candidates.begin(), candidates.begin() + i, candidate) != candidates.begin() + i) { continue; }
            candidates[i++] = candidate;
        }
    }
    return candidates;
}
UrmaJfsSlot* UrmaResourcePool::PickBackup(uint32_t preferred) const {
    if (io_jfs.size() <= 1) { errno = EAGAIN; return nullptr; }
    const auto candidates = SampleBackups(preferred);
    const size_t count = std::min<size_t>(4, io_jfs.size() - 1);
    UrmaJfsSlot* best = nullptr;
    uint32_t lowest = UINT32_MAX;
    for (size_t i = 0; i < count; ++i) {
        auto* slot = io_jfs[candidates[i]];
        const uint32_t used = slot->sq_inflight.load(std::memory_order_relaxed);
        if (!slot->stopped.load() && used < lowest) { lowest = used; best = slot; }
    }
    if (!best || lowest >= best->sq_depth) { errno = EAGAIN; return nullptr; }
    return best;
}
bool UrmaResourcePool::IsWritable(uint32_t preferred) const {
    if (_writers_closed.load() || io_jfs.empty()) { return false; }
    auto* slot = io_jfs[preferred % io_jfs.size()];
    return !slot->stopped.load() && slot->sq_inflight.load() < slot->sq_depth;
}
UrmaWriteWaiter::UrmaWriteWaiter(UrmaResourcePool* p, butil::atomic<int>* b,
                               UrmaJfsSlot* s)
    : pool(p), slot(s ? s : p->io_jfs[0]), butex(b) {
    pool->_active_writers.fetch_add(1);
}
UrmaWriteWaiter::~UrmaWriteWaiter() {
    const int saved_errno = errno;
    {
        BAIDU_SCOPED_LOCK(slot->writers_mutex);
        pool->RemoveWriterLocked(this);
    }
    pool->_active_writers.fetch_sub(1);
    errno = saved_errno;
}
void UrmaResourcePool::RemoveWriterLocked(WriteWaiter* waiter) {
    if (!waiter->queued) { return; }
    auto* slot = waiter->slot;
    if (waiter->prev) { waiter->prev->next = waiter->next; }
    else { slot->writers = waiter->next; }
    if (waiter->next) { waiter->next->prev = waiter->prev; }
    waiter->queued = false;
    slot->has_writers.store(slot->writers != nullptr);
}
void UrmaResourcePool::WakeWritersLocked(UrmaJfsSlot* slot, bool all) {
    while (slot->writers) {
        auto* waiter = slot->writers;
        RemoveWriterLocked(waiter);
        waiter->butex->fetch_add(1, butil::memory_order_release);
        bthread::butex_wake_except(waiter->butex, INVALID_BTHREAD);
        if (!all) { break; }
    }
}
int UrmaResourcePool::WaitWritable(butil::atomic<int>* butex, int expected,
                                 const timespec* deadline, uint32_t preferred) {
    UrmaOperation active(&operations);
    if (!active || io_jfs.empty()) { errno = EIO; return -1; }
    auto* slot = io_jfs[preferred % io_jfs.size()];
    WriteWaiter waiter(this, butex, slot);
    {
        BAIDU_SCOPED_LOCK(slot->writers_mutex);
        if (_writers_closed.load() || slot->stopped.load()) { errno = EIO; return -1; }
        waiter.next = slot->writers;
        if (slot->writers) { slot->writers->prev = &waiter; }
        slot->writers = &waiter;
        waiter.queued = true;
        // Publish before rechecking space; pair with SC credit release.
        slot->has_writers.store(true);
        if (IsWritable(preferred)) { RemoveWriterLocked(&waiter); return 0; }
    }
    return bthread::butex_wait(butex, expected, deadline);
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
        slot->sq_inflight.fetch_sub(1);
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
            slot->sq_inflight.fetch_sub(1);
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
        if (slot->has_writers.load()) {
            BAIDU_SCOPED_LOCK(slot->writers_mutex);
            if (slot->stopped.load() || _writers_closed.load()) {
                WakeWritersLocked(slot, true);
            } else if (IsWritable(slot->index)) {
                WakeWritersLocked(slot);
            }
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
    slot->sq_inflight.fetch_sub(1);
    if (cr.status != URMA_CR_SUCCESS) {
        FailTxContext(ctx, EIO, "URMA send completion failed");
    }
    ctx->Release();
    KickDrain(slot);
}

int UrmaResourcePool::PostRecvToSlot(UrmaJfrSlot& slot, uint32_t num) {
    if (slot.contexts || num != slot.rx_depth) { errno = EINVAL; return -1; }
    slot.contexts.reset(new RxRecvCtx[num]);
    for (uint32_t i = 0; i < num; ++i) {
        auto* context = &slot.contexts[i];
        context->jfr_index = slot.index;
        context->header_buf = static_cast<UrmaMsgHeader*>(AllocHeaderBuffer());
        if (!context->header_buf) { errno = ENOMEM; return -1; }
        std::memset(context->header_buf, 0, sizeof(UrmaMsgHeader));
        if (RepostRecv(slot, context) != 0) { return -1; }
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
    context->posted = true;
    ++slot.rx_posted;
    return 0;
}

bool UrmaResourcePool::Shutdown() {
    operations.Close();
    _writers_closed.store(true);
    for (auto* slot : io_jfs) {
        BAIDU_SCOPED_LOCK(slot->writers_mutex);
        WakeWritersLocked(slot, true);
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
        if (slot->jfr && urma_delete_jfr(slot->jfr) != URMA_SUCCESS) {
            success = false;
            continue;
        }
        slot->jfr = nullptr;
        slot->contexts.reset();
        slot->rx_posted = 0;
    }
    if (!success) { return false; }
    std::vector<UrmaIoSlot*> slots(io_jfs.begin(), io_jfs.end());
    slots.insert(slots.end(), io_jfr.begin(), io_jfr.end());
    for (auto* slot : slots) {
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
