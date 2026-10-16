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

#if BRPC_WITH_URMA
#include <cerrno>
#include <cstring>
#include <gflags/gflags.h>
#include "butil/fd_utility.h"
#include "butil/scoped_lock.h"
#include "brpc/urma/urma_helper.h"

namespace brpc {
namespace urma {
DEFINE_int32(urma_io_jfs_cnt, 128, "Number of process-wide IO JFS slots");
DEFINE_int32(urma_io_jfr_cnt, 64, "Number of process-wide shared IO JFR slots");
DEFINE_int32(urma_io_sq_depth, 128, "Depth of each pooled IO send queue");
DEFINE_int32(urma_io_rx_depth, 512, "Depth of each shared IO receive queue");
UrmaResourcePool* g_urma_pool = nullptr;
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
int UrmaResourcePool::PostSend(UrmaJfsSlot* slot, urma_jfs_wr_t* wr,
                               TxCompletionCtx* context) {
    BAIDU_SCOPED_LOCK(slot->mutex);
    if (slot->stopped.load()) { errno = EIO; return -1; }
    slot->pending.insert(context);
    urma_jfs_wr_t* bad = nullptr;
    const urma_status_t rc = urma_post_jfs_wr(slot->jfs, wr, &bad);
    if (rc != URMA_SUCCESS) {
        slot->pending.erase(context);
        errno = rc > 0 ? rc : EIO;
        return -1;
    }
    return 0;
}
int UrmaResourcePool::PostRecvToSlot(UrmaJfrSlot& slot, uint32_t num) {
    for (uint32_t i = 0; i < num; ++i) {
        std::unique_ptr<RxRecvCtx> context(new RxRecvCtx);
        context->jfr_index = slot.index;
        context->header_buf = static_cast<UrmaMsgHeader*>(AllocHeaderBuffer());
        if (!context->header_buf) { errno = ENOMEM; return -1; }
        std::memset(context->header_buf, 0, sizeof(UrmaMsgHeader));
        const size_t block_size = GetUrmaRecvBlockSize();
        // Destroy the stream BEFORE publishing the context: its destructor
        // may still access the IOBuf, and a CQE can arrive immediately.
        {
            butil::IOBufAsZeroCopyOutputStream stream(
                &context->data_iobuf, block_size + sizeof(butil::IOBuf::Block));
            int size = 0;
            if (!stream.Next(&context->data_block, &size) ||
                size < static_cast<int>(block_size)) { errno = ENOMEM; return -1; }
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
        wr.user_ctx = reinterpret_cast<uint64_t>(context.get());
        urma_jfr_wr_t* bad = nullptr;
        slot.pending.insert(context.get());
        if (urma_post_jfr_wr(slot.jfr, &wr, &bad) != URMA_SUCCESS) {
            slot.pending.erase(context.get());
            errno = EIO;
            return -1;
        }
        context.release();
    }
    return 0;
}

bool UrmaResourcePool::Shutdown() {
    bool success = true;
    for (auto* slot : io_jfs) {
        BAIDU_SCOPED_LOCK(slot->mutex);
        slot->stopped.store(true);
        if (_events.detach) { _events.detach(slot); }
        // Successful deletion is the DMA quiescence boundary. Retain all
        // storage if the provider cannot stop this queue.
        if (slot->jfs && urma_delete_jfs(slot->jfs) != URMA_SUCCESS) {
            success = false;
            continue;
        }
        slot->jfs = nullptr;
        for (auto* ctx : slot->pending) { delete ctx; }
        slot->pending.clear();
        slot->sq_inflight.store(0);
    }
    for (auto* slot : io_jfr) {
        BAIDU_SCOPED_LOCK(slot->mutex);
        slot->stopped.store(true);
        if (_events.detach) { _events.detach(slot); }
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
        BAIDU_SCOPED_LOCK(slot->mutex);
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
