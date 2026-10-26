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
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <tuple>
#include <vector>
#include "brpc/socket.h"
#include "brpc/urma/urma_memory_pool.h"
#include "butil/synchronization/lock.h"
#include "urma_api.h"

#ifndef BRPC_URMA_SINGLE_SGE
#define BRPC_URMA_SINGLE_SGE 0
#endif

namespace brpc {
namespace urma {
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
    uint32_t local_jfr_index{0};
    urma_target_jetty_t* remote_jfr{nullptr};
    urma_target_seg_t* remote_seg{nullptr};
    std::shared_ptr<ImportedJfrCache> imports;
    ~UrmaConnResource();
};

struct TxCompletionCtx {
    SocketId self_socket_id{INVALID_SOCKET_ID};
    uint32_t jfs_index{0};
    UrmaMsgHeader* header_buf{nullptr};
    butil::IOBuf send_data;
    // A disconnected endpoint may disappear before DMA finishes. Keep its
    // imports, as well as its data, alive through the final CQE.
    std::shared_ptr<UrmaConnResource> connection;
    uint16_t seq{0};
    uint8_t type{MSG_SMALL};
    ~TxCompletionCtx() { FreeHeaderBuffer(header_buf); }
};
struct RxRecvCtx {
    UrmaMsgHeader* header_buf{nullptr};
    void* data_block{nullptr};
    butil::IOBuf data_iobuf;
    uint32_t jfr_index{0};
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
    // Serializes poll/post against teardown, not across different slots.
    butil::Mutex mutex;
    std::atomic<bool> stopped{false};
private:
    std::atomic<uint32_t> refs{1};
};
struct UrmaJfsSlot : public UrmaIoSlot {
    explicit UrmaJfsSlot(uint32_t i) : UrmaIoSlot(JFS_SLOT, i) {}
    urma_jfs_t* jfs{nullptr};
    std::atomic<uint32_t> sq_inflight{0};
    uint32_t sq_depth{0};
    std::set<TxCompletionCtx*> pending;
};
struct UrmaJfrSlot : public UrmaIoSlot {
    explicit UrmaJfrSlot(uint32_t i) : UrmaIoSlot(JFR_SLOT, i) {}
    urma_jfr_t* jfr{nullptr};
    uint32_t rx_depth{0};
    uint32_t jfr_id{0};
    urma_token_t token_value{};
    std::set<RxRecvCtx*> pending;
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
    virtual uint32_t PickJfr();
    bool IsWritable() const;
    virtual int PostSend(UrmaJfsSlot*, urma_jfs_wr_t*, TxCompletionCtx*);
    // Caller holds slot.mutex when invoked by the completion handler.
    virtual int PostRecvToSlot(UrmaJfrSlot&, uint32_t num);
    // Reuse a completed receive context under slot.mutex. On failure it remains
    // owned by slot.pending, so shutdown still controls the DMA lifetime.
    virtual int RepostRecv(UrmaJfrSlot&, RxRecvCtx*);
    std::vector<UrmaJfsSlot*> io_jfs;
    std::vector<UrmaJfrSlot*> io_jfr;
    std::atomic<uint32_t> jfs_rr_counter{0};
    std::atomic<uint32_t> jfr_rr_counter{0};
    std::shared_ptr<ImportedJfrCache> imports{new ImportedJfrCache};
private:
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
