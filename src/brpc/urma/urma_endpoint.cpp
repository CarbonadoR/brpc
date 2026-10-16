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

#include "brpc/urma/urma_endpoint.h"

#if BRPC_WITH_URMA

#include <unistd.h>

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <gflags/gflags.h>

#include "butil/atomicops.h"
#include "butil/containers/stack_container.h"
#include "butil/iobuf.h"
#include "butil/logging.h"
#include "butil/macros.h"
#include "butil/sys_byteorder.h"
#include "butil/time.h"
#include "bthread/bthread.h"
#include "bthread/butex.h"

#include "urma_api.h"

#include "brpc/input_messenger.h"
#include "brpc/socket.h"
#include "brpc/urma/urma_handshake.h"
#include "brpc/urma/urma_handshake.pb.h"
#include "brpc/urma/urma_helper.h"
#include "brpc/urma_transport.h"


namespace brpc {
namespace urma {

// Flags used here are declared in urma_endpoint.h (urma_use_polling,
// urma_poller_num, urma_disable_bthread). Declare the rest here.
DECLARE_int32(urma_cqe_poll_once);
DECLARE_bool(urma_recv_zerocopy);
DECLARE_int32(urma_zerocopy_min_size);

// ---- Constants shared with the handshake module ----
static const int WAIT_TIMEOUT_MS = 50;
static const size_t HELLO_ACK_LEN = 4;
static const uint32_t HELLO_ACK_URMA_OK = 0x1;
static const size_t IOBUF_BLOCK_HEADER_LEN = sizeof(butil::IOBuf::Block);

UrmaEndpoint::UrmaEndpoint(Socket* s) : _socket(s) {
    _read_butex = bthread::butex_create_checked<butil::atomic<int>>();
    _read_butex->store(0, butil::memory_order_relaxed);
    _input_processor.Init(s, InputMessengerProcessor::STREAM_URMA_JETTY);
}

UrmaEndpoint::~UrmaEndpoint() {
    DeallocateResources();
    if (_read_butex) {
        bthread::butex_destroy(_read_butex);
        _read_butex = nullptr;
    }
}

void UrmaEndpoint::Reset() {
    DeallocateResources();
    _state = UNINIT;
    _handshake_version = 0;
    _remote_recv_block_size = 0;
    _self_socket_id = INVALID_SOCKET_ID;
    _peer_socket_id = INVALID_SOCKET_ID;
    _read_butex->store(0, butil::memory_order_relaxed);
}

// ============================================================================
// Handshake IO helpers (ReadFromFd / WriteToFd / PushBackToReadBuf).
// Modeled on RdmaEndpoint::ReadFromFdLoop / WriteToFdLoop.
// ============================================================================

int UrmaEndpoint::ReadFromFd(void* data, size_t len) {
    char* p = static_cast<char*>(data);
    size_t received = 0;
    while (received < len) {
        const int expected_val = _read_butex->load(butil::memory_order_acquire);
        const timespec duetime = butil::milliseconds_from_now(WAIT_TIMEOUT_MS);
        const int fd = _socket->fd();
        const ssize_t nr = read(fd, p + received, len - received);
        if (nr < 0) {
            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                int rc = bthread::butex_wait(_read_butex, expected_val, &duetime);
                if (rc < 0 && errno != EWOULDBLOCK && errno != ETIMEDOUT) {
                    return -1;
                }
                continue;
            }
            return -1;
        }
        if (nr == 0) {
            errno = EEOF;
            return -1;
        }
        received += nr;
    }
    return 0;
}

void UrmaEndpoint::PushBackToReadBuf(const void* data, size_t len) {
    _socket->fd_input_processor().read_buf().append(data, len);
}

int UrmaEndpoint::WriteToFd(void* data, size_t len) {
    char* p = static_cast<char*>(data);
    size_t written = 0;
    while (written < len) {
        const timespec duetime = butil::milliseconds_from_now(WAIT_TIMEOUT_MS);
        const int fd = _socket->fd();
        const ssize_t nw = write(fd, p + written, len - written);
        if (nw >= 0) {
            written += nw;
            continue;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            return -1;
        }
        if (_socket->WaitEpollOut(fd, true, &duetime) != 0 && errno != ETIMEDOUT) {
            return -1;
        }
    }
    return 0;
}

// ============================================================================
// Hello builders / parsers.
// ============================================================================

void UrmaEndpoint::MakeLocalParsedHello(ParsedHello* out) const {
    *out = ParsedHello{};  // value-initialize (avoids memset on non-trivial type)
    out->buffer_size = static_cast<uint32_t>(GetUrmaRecvBlockSize());
    if (_conn_res && g_urma_pool) {
        const UrmaJfrSlot* slot = g_urma_pool->io_jfr[_conn_res->local_jfr_index];
        out->recv_buffer_cnt = slot->rx_depth;
        out->jfr_id = slot->jfr_id;
        out->uasid = slot->jfr->jfr_id.uasid;
        const urma_eid_t* eid = GetUrmaLocalEid();
        std::memcpy(out->eid, eid ? eid->raw : slot->jfr->jfr_id.eid.raw, 16);
    }
    out->socket_id = _self_socket_id;
    out->tp_type = static_cast<uint8_t>(URMA_CTP);
    // Pool segment: flatten g_pool_seg's seg fields.
    urma_target_seg_t* pool = GetPoolSegFor(nullptr);
    if (pool) {
        std::memcpy(out->seg_eid, pool->seg.ubva.eid.raw, 16);
        out->seg_uasid = pool->seg.ubva.uasid;
        out->seg_va = pool->seg.ubva.va;
        out->seg_len = pool->seg.len;
        out->seg_token_id = pool->seg.token_id;
    }
}

void UrmaEndpoint::FillLocalHelloV2(v2_wire::HelloMessage* out) const {
    *out = v2_wire::HelloMessage{};  // value-initialize
    out->msg_len = v2_wire::HELLO_PACKET_LEN;
    out->hello_ver = v2_wire::HELLO_V2_VERSION;
    out->impl_ver = v2_wire::IMPL_V2_VERSION;
    ParsedHello p;
    MakeLocalParsedHello(&p);
    out->buffer_size = p.buffer_size;
    out->recv_buffer_cnt = p.recv_buffer_cnt;
    out->jfr_id = p.jfr_id;
    out->socket_id = p.socket_id;
    std::memcpy(out->eid, p.eid, 16);
    out->uasid = p.uasid;
    out->tp_type = p.tp_type;
    std::memcpy(out->seg_eid, p.seg_eid, 16);
    out->seg_uasid = p.seg_uasid;
    out->seg_va = p.seg_va;
    out->seg_len = p.seg_len;
    out->seg_token_id = p.seg_token_id;
}

void UrmaEndpoint::FillLocalHelloV3(UrmaHello* out) const {
    ParsedHello p;
    MakeLocalParsedHello(&p);
    out->set_buffer_size(p.buffer_size);
    out->set_recv_buffer_cnt(p.recv_buffer_cnt);
    out->set_jfr_id(p.jfr_id);
    out->set_socket_id(p.socket_id);
    out->set_impl_version(v2_wire::IMPL_V2_VERSION);
    out->set_eid(p.eid, 16);
    out->set_uasid(p.uasid);
    out->set_tp_type(p.tp_type);
    out->set_seg_eid(p.seg_eid, 16);
    out->set_seg_uasid(p.seg_uasid);
    out->set_seg_va(p.seg_va);
    out->set_seg_len(p.seg_len);
    out->set_seg_token_id(p.seg_token_id);
}

int UrmaEndpoint::WriteHelloV3(const UrmaHello& msg) {
    butil::IOBuf packet;
    packet.append("URM3", 4);
    std::string body;
    if (!msg.SerializeToString(&body)) {
        LOG(ERROR) << "Fail to serialize UrmaHello";
        return -1;
    }
    uint32_t pb_size_be = butil::HostToNet32(static_cast<uint32_t>(body.size()));
    packet.append(&pb_size_be, sizeof(pb_size_be));
    packet.append(body);
    return WriteToFd(packet);
}

int UrmaEndpoint::WriteToFd(butil::IOBuf& data) {
    // Write out the IOBuf in a single WriteToFd-style loop.
    while (!data.empty()) {
        const timespec duetime = butil::milliseconds_from_now(WAIT_TIMEOUT_MS);
        const int fd = _socket->fd();
        const ssize_t nw = data.cut_into_file_descriptor(fd);
        if (nw >= 0) {
            continue;
        }
        if (errno != EAGAIN && errno != EWOULDBLOCK) {
            return -1;
        }
        if (_socket->WaitEpollOut(fd, true, &duetime) != 0 && errno != ETIMEDOUT) {
            return -1;
        }
    }
    return 0;
}

int UrmaEndpoint::ReadAndParseHelloV3(ParsedHello* out, bool* negotiated) {
    *negotiated = false;
    uint32_t pb_size_be = 0;
    if (ReadFromFd(&pb_size_be, sizeof(pb_size_be)) < 0) {
        return -1;
    }
    const uint32_t pb_size = butil::NetToHost32(pb_size_be);
    if (pb_size == 0 || pb_size > 4096) {
        return 0;
    }
    std::string body(pb_size, '\0');
    if (ReadFromFd(&body[0], pb_size) < 0) {
        return -1;
    }
    UrmaHello msg;
    if (!msg.ParseFromArray(body.data(), static_cast<int>(body.size()))) {
        return 0;
    }
    if (msg.eid().size() != 16 || msg.seg_eid().size() != 16 ||
        !msg.has_socket_id() || msg.impl_version() != v2_wire::IMPL_V2_VERSION) {
        return 0;
    }
    out->buffer_size = msg.buffer_size();
    out->recv_buffer_cnt = msg.recv_buffer_cnt();
    out->jfr_id = msg.jfr_id();
    out->socket_id = msg.socket_id();
    std::memcpy(out->eid, msg.eid().data(), 16);
    out->uasid = msg.uasid();
    out->tp_type = static_cast<uint8_t>(msg.tp_type());
    std::memcpy(out->seg_eid, msg.seg_eid().data(), 16);
    out->seg_uasid = msg.seg_uasid();
    out->seg_va = msg.seg_va();
    out->seg_len = msg.seg_len();
    out->seg_token_id = msg.seg_token_id();
    if (!ValidHello(*out)) {
        return 0;
    }
    *negotiated = true;
    return 0;
}

// ============================================================================
// Allocate / deallocate per-connection resources.
// ============================================================================

int UrmaEndpoint::AllocateResources() {
    if (_conn_res) { return 0; }
    if (!g_urma_pool || g_urma_pool->io_jfr.empty()) {
        errno = ENODEV;
        return -1;
    }
    _conn_res.reset(new UrmaConnResource);
    _conn_res->local_jfr_index = g_urma_pool->PickJfr();
    _conn_res->imports = g_urma_pool->imports;
    _self_socket_id = _socket->id();
    return 0;
}

void UrmaEndpoint::DeallocateResources() {
    BAIDU_SCOPED_LOCK(_reorder_mutex);
    BAIDU_SCOPED_LOCK(_dispatch_mutex);
    // In-flight TxCompletionCtx objects retain their own connection reference.
    _conn_res.reset();
    _reorder_queue.clear();
    _deliver_base = 0;
    _next_send_seq = 0;
    _pending_received_bytes.store(0, butil::memory_order_relaxed);
    _input_processor.Reset();
}

int UrmaEndpoint::ImportPeer(const ParsedHello& peer) {
    urma_context_t* context = GetUrmaContext();
    if (!context || !_conn_res) { errno = ENODEV; return -1; }
    // Retain the established import_seg-before-import_jfr ordering. SEND does
    // not use remote memory, but some providers need it to establish routing.
    urma_seg_t segment{};
    std::memcpy(segment.ubva.eid.raw, peer.seg_eid, 16);
    segment.ubva.uasid = peer.seg_uasid;
    segment.ubva.va = peer.seg_va;
    segment.len = peer.seg_len;
    segment.token_id = peer.seg_token_id;
    urma_token_t token{};
    urma_import_seg_flag_t flag{};
    flag.bs.cacheable = URMA_NON_CACHEABLE;
    flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE | URMA_ACCESS_ATOMIC;
    flag.bs.mapping = URMA_SEG_NOMAP;
    _conn_res->remote_seg = urma_import_seg(context, &segment, &token, 0, flag);
    if (!_conn_res->remote_seg) { return -1; }
    urma_rjfr_t remote{};
    std::memcpy(remote.jfr_id.eid.raw, peer.eid, 16);
    remote.jfr_id.uasid = peer.uasid;
    remote.jfr_id.id = peer.jfr_id;
    remote.trans_mode = URMA_TM_RM;
    remote.tp_type = URMA_CTP;
    remote.flag.bs.token_policy = URMA_TOKEN_NONE;
    _conn_res->remote_jfr = _conn_res->imports->Acquire(context, &remote, &token);
    return _conn_res->remote_jfr ? 0 : -1;
}

// Gather across IOBufs until either the JFS SGE limit (including the header)
// or the CTP body limit is reached. Keep caller data intact until post succeeds.
ssize_t UrmaEndpoint::CutFromIOBufList(butil::IOBuf** from, size_t ndata) {
    if (!_conn_res || !_conn_res->remote_jfr || !g_urma_pool) {
        errno = ENOTCONN;
        return -1;
    }
    ssize_t total = 0;
    butil::StackVector<urma_sge_t, 16> sge;
    for (size_t i = 0; i < ndata; ) {
        if (from[i]->empty()) { ++i; continue; }
        UrmaJfsSlot* slot = g_urma_pool->AcquireJfs();
        if (!slot) { return total ? total : -1; }
        std::unique_ptr<TxCompletionCtx> ctx(new TxCompletionCtx);
        ctx->self_socket_id = _self_socket_id;
        ctx->jfs_index = slot->index;
        ctx->connection = _conn_res;
        ctx->seq = _next_send_seq;
        ctx->header_buf = static_cast<UrmaMsgHeader*>(AllocHeaderBuffer());
        auto fail = [&](int error) -> ssize_t {
            slot->sq_inflight.fetch_sub(1);
            errno = error;
            return total ? total : -1;
        };
        if (!ctx->header_buf) { return fail(ENOMEM); }
        std::memset(ctx->header_buf, 0, sizeof(UrmaMsgHeader));
        ctx->header_buf->dst_socket_id = butil::HostToNet64(_peer_socket_id);
        ctx->header_buf->seq = butil::HostToNet16(ctx->seq);
        ctx->header_buf->type = MSG_SMALL;
        size_t limit = std::min<size_t>(_remote_recv_block_size, GetUrmaRecvBlockSize());
        limit = std::min(limit, URMA_CTP_MAX_MESSAGE_SIZE - sizeof(UrmaMsgHeader));
#if BRPC_URMA_SINGLE_SGE
        // One local registered block contains the concatenated wire bytes.
        // Receives always retain two SGEs (64-byte header, body).
        if (GetUrmaRecvBlockSize() <= sizeof(UrmaMsgHeader)) { return fail(EMSGSIZE); }
        limit = std::min(limit, GetUrmaRecvBlockSize() - sizeof(UrmaMsgHeader));
#endif
        if (!limit) { return fail(EMSGSIZE); }
        size_t len = 0;
        sge->clear();
#if BRPC_URMA_SINGLE_SGE
        void* wire = nullptr;
        {
            butil::IOBufAsZeroCopyOutputStream stream(
                &ctx->send_data, GetUrmaRecvBlockSize() + IOBUF_BLOCK_HEADER_LEN);
            int size = 0;
            if (!stream.Next(&wire, &size) ||
                size < static_cast<int>(sizeof(UrmaMsgHeader) + limit)) {
                return fail(ENOMEM);
            }
            std::memcpy(wire, ctx->header_buf, sizeof(UrmaMsgHeader));
            // Input block boundaries do not consume SGEs in the copying mode.
            for (size_t j = i; j < ndata && len < limit; ++j) {
                len += from[j]->copy_to(
                    static_cast<char*>(wire) + sizeof(UrmaMsgHeader) + len,
                    limit - len);
            }
            stream.BackUp(size - sizeof(UrmaMsgHeader) - len);
        }
        sge->push_back(urma_sge_t{});
        sge[0].addr = reinterpret_cast<uint64_t>(wire);
        sge[0].len = sizeof(UrmaMsgHeader) + len;
        sge[0].tseg = GetPoolSegFor(wire);
#else
        const uint32_t max_sge = slot->jfs->jfs_cfg.max_sge;
        if (max_sge < 2) { return fail(EINVAL); }
        sge->push_back(urma_sge_t{});
        sge[0].addr = reinterpret_cast<uint64_t>(ctx->header_buf);
        sge[0].len = sizeof(UrmaMsgHeader);
        sge[0].tseg = GetHeaderPoolSegFor(ctx->header_buf);
        for (size_t j = i; j < ndata && len < limit && sge->size() < max_sge; ++j) {
            size_t offset = 0;
            for (size_t block = 0; block < from[j]->backing_block_num() &&
                    len < limit && sge->size() < max_sge; ++block) {
                const butil::StringPiece piece = from[j]->backing_block(block);
                urma_sge_t body{};
                body.addr = reinterpret_cast<uint64_t>(piece.data());
                body.len = std::min(piece.size(), limit - len);
                body.tseg = GetPoolSegFor(const_cast<char*>(piece.data()));
                if (!body.tseg) {
                    // User-registered blocks can have different segment handles
                    // within the same IOBuf. Resolve the metadata at this offset.
                    butil::IOBuf first_byte;
                    from[j]->append_to(&first_byte, 1, offset);
                    body.tseg = reinterpret_cast<urma_target_seg_t*>(
                        static_cast<uintptr_t>(first_byte.get_first_data_meta()));
                }
                sge->push_back(body);
                len += body.len;
                offset += body.len;
            }
            // Retain the gathered prefix in one pass, without copying bytes.
            from[j]->append_to(&ctx->send_data, offset);
        }
#endif
        for (const auto& part : sge.container()) {
            if (!part.tseg) { return fail(ERDMAMEM); }
        }
        urma_jfs_wr_t wr{};
        wr.opcode = URMA_OPC_SEND;
        wr.flag.bs.complete_enable = 1;
        wr.tjetty = _conn_res->remote_jfr;
        wr.send.src = urma_sg_t{sge->data(), static_cast<uint32_t>(sge->size())};
        wr.user_ctx = reinterpret_cast<uint64_t>(ctx.get());
        // PostSend serializes publication with CQ draining. After it returns,
        // the CQE may already have destroyed ctx: release without dereferencing.
        if (g_urma_pool->PostSend(slot, &wr, ctx.get()) != 0) {
            return fail(errno);
        }
        ctx.release();
        size_t remaining = len;
        while (remaining != 0) {
            remaining -= from[i]->pop_front(remaining);
            if (from[i]->empty()) { ++i; }
        }
        ++_next_send_seq;
        total += len;
    }
    return total;
}

bool UrmaEndpoint::IsWritable() const {
    return g_urma_pool && g_urma_pool->IsWritable();
}

void UrmaEndpoint::HandleTxCompletion(UrmaJfsSlot* slot, const urma_cr_t& cr) {
    auto* ctx = reinterpret_cast<TxCompletionCtx*>(cr.user_ctx);
    if (!ctx || slot->pending.erase(ctx) != 1) {
        LOG(ERROR) << "Unknown URMA TX completion context";
        slot->stopped.store(true);
        return;
    }
    const SocketId sid = ctx->self_socket_id;
    delete ctx;
    slot->sq_inflight.fetch_sub(1);
    SocketUniquePtr socket;
    if (Socket::Address(sid, &socket) == 0) {
        if (cr.status != URMA_CR_SUCCESS) {
            socket->SetFailed(EIO, "URMA send completion status=%d", cr.status);
        } else {
            socket->WakeAsEpollOut();
        }
    }
}

void UrmaEndpoint::HandleRxCompletion(UrmaJfrSlot* slot, const urma_cr_t& cr) {
    auto* raw = reinterpret_cast<RxRecvCtx*>(cr.user_ctx);
    if (!raw || slot->pending.erase(raw) != 1) {
        LOG(ERROR) << "Unknown URMA RX completion context";
        slot->stopped.store(true);
        return;
    }
    std::unique_ptr<RxRecvCtx> ctx(raw);
    SocketUniquePtr socket;
    UrmaEndpoint* ep = nullptr;
    uint16_t seq = 0;
    butil::IOBuf data;
    // Validate the total length before reading the header or subtracting 64.
    if (cr.status == URMA_CR_SUCCESS && cr.opcode == URMA_CR_OPC_SEND &&
        cr.completion_len > sizeof(UrmaMsgHeader) &&
        cr.completion_len <= sizeof(UrmaMsgHeader) + GetUrmaRecvBlockSize() &&
        ctx->header_buf->type == MSG_SMALL) {
        const SocketId sid = butil::NetToHost64(ctx->header_buf->dst_socket_id);
        seq = butil::NetToHost16(ctx->header_buf->seq);
        if (Socket::Address(sid, &socket) == 0 && socket->_transport &&
            socket->socket_mode() == SOCKET_MODE_URMA) {
            auto* transport = static_cast<UrmaTransport*>(socket->_transport.get());
            ep = transport->_urma_ep;
            if (ep) {
                const size_t len = cr.completion_len - sizeof(UrmaMsgHeader);
                if (FLAGS_urma_recv_zerocopy &&
                    len >= static_cast<size_t>(FLAGS_urma_zerocopy_min_size)) {
                    ctx->data_iobuf.cutn(&data, len);
                } else {
                    data.append(ctx->data_block, len);
                }
            }
        }
    }
    // Reclaim before reposting so an exactly sized header pool also works.
    ctx.reset();
    if (g_urma_pool->PostRecvToSlot(*slot, 1) != 0) {
        slot->stopped.store(true);
        LOG(ERROR) << "Failed to replenish shared URMA JFR " << slot->index;
        if (socket) { socket->SetFailed(EIO, "URMA receive replenishment failed"); }
        return;
    }
    if (ep) { ep->ReorderAndDispatch(socket, seq, std::move(data)); }
}

void UrmaEndpoint::ReorderAndDispatch(SocketUniquePtr& socket, uint16_t seq,
                                      butil::IOBuf data) {
    BAIDU_SCOPED_LOCK(_reorder_mutex);
    BAIDU_SCOPED_LOCK(_dispatch_mutex);
    const State state = _state.load(butil::memory_order_acquire);
    if (!_conn_res || state == FAILED || state == FALLBACK_TCP || socket->Failed()) {
        return;
    }
    const uint16_t distance = static_cast<uint16_t>(seq - _deliver_base);
    // A 16-bit sequence can only order the forward half of its space. Never
    // silently overwrite a duplicate or append stale data to the stream.
    if (distance >= 32768 || _reorder_queue.count(seq)) {
        socket->SetFailed(EPROTO, "Invalid/duplicate URMA sequence %u base=%u",
                           seq, _deliver_base);
        return;
    }
    _reorder_queue.emplace(seq, std::move(data));
    ssize_t bytes = 0;
    // Lookup the expected key, not map.begin(): numeric sort is wrong at wrap.
    for (auto it = _reorder_queue.find(_deliver_base);
         it != _reorder_queue.end(); it = _reorder_queue.find(_deliver_base)) {
        bytes += it->second.size();
        _input_processor.read_buf().append(it->second);
        _reorder_queue.erase(it);
        ++_deliver_base;
    }
    DispatchReceivedBytesLocked(socket, bytes);
}

void UrmaEndpoint::DispatchReceivedBytes(SocketUniquePtr& socket, ssize_t bytes) {
    BAIDU_SCOPED_LOCK(_dispatch_mutex);
    DispatchReceivedBytesLocked(socket, bytes);
}
void UrmaEndpoint::DispatchReceivedBytesLocked(SocketUniquePtr& socket,
                                               ssize_t bytes) {
    if (bytes > 0) { _pending_received_bytes.fetch_add(bytes); }
    if (_state.load(butil::memory_order_acquire) != ESTABLISHED || socket->Failed()) {
        return;
    }
    const int64_t pending = _pending_received_bytes.exchange(0);
    if (pending <= 0 || !socket->user()) { return; }
    const int64_t now = butil::cpuwide_time_us();
    InputMessageClosure last_msg;
    _input_processor.ProcessNewMessage(pending, false, now,
        butil::gettimeofday_us() - now, last_msg);
}

int UrmaEndpoint::AttachSlotSocket(UrmaIoSlot* slot) {
    // First create a carrier without a user or fd. A failed Socket::Create
    // may invoke BeforeRecycle, so transfer slot ownership only after success.
    SocketOptions options;
    options.on_edge_triggered_events = PollIoJfc;
    if (Socket::Create(options, &slot->jfc_sid) != 0) { return -1; }
    SocketUniquePtr socket;
    if (Socket::Address(slot->jfc_sid, &socket) != 0) { return -1; }
    slot->AddRef();
    socket->_user = slot;
    // Keep liburma's fd ownership independent of the carrier's lifetime.
    const int fd = dup(slot->jfce->fd);
    if (fd < 0 || socket->ResetFileDescriptor(fd) != 0) {
        if (fd >= 0) { close(fd); }
        socket->SetFailed();
        slot->jfc_sid = INVALID_SOCKET_ID;
        return -1;
    }
    return 0;
}
void UrmaEndpoint::DetachSlotSocket(UrmaIoSlot* slot) {
    if (slot->jfc_sid == INVALID_SOCKET_ID) { return; }
    SocketUniquePtr socket;
    if (Socket::Address(slot->jfc_sid, &socket) == 0) {
        if (socket->_fd >= 0) { socket->_io_event.RemoveConsumer(socket->_fd); }
        // Socket closes its duplicate; the provider owns the original fd.
        socket->SetFailed();
    }
    slot->jfc_sid = INVALID_SOCKET_ID;
}
void UrmaEndpoint::PollIoJfc(Socket* carrier) {
    auto* slot = static_cast<UrmaIoSlot*>(carrier->user());
    BAIDU_SCOPED_LOCK(slot->mutex);
    if (slot->stopped.load()) { return; }
    auto drain = [&]() -> bool {
        while (!slot->stopped.load()) {
            urma_cr_t cr[32];
            const int count = urma_poll_jfc(slot->jfc,
                std::max(1, std::min(32, FLAGS_urma_cqe_poll_once)), cr);
            if (count < 0) { return false; }
            if (!count) { return true; }
            for (int i = 0; i < count; ++i) {
                if (slot->type == UrmaIoSlot::JFS_SLOT) {
                    HandleTxCompletion(static_cast<UrmaJfsSlot*>(slot), cr[i]);
                } else {
                    HandleRxCompletion(static_cast<UrmaJfrSlot*>(slot), cr[i]);
                }
            }
        }
        return false;
    };
    int progress = Socket::PROGRESS_INIT;
    while (true) {
        urma_jfc_t* event = nullptr;
        const int count = urma_wait_jfc(slot->jfce, 1, 0, &event);
        if (count < 0 && errno == EINTR) { continue; }
        if (count < 0 && errno != EAGAIN && errno != EWOULDBLOCK) {
            slot->stopped.store(true);
            return;
        }
        if (count > 0) {
            const bool ok = drain();
            uint32_t events = 1;
            urma_ack_jfc(&event, &events, 1);
            if (!ok || urma_rearm_jfc(slot->jfc, false) != URMA_SUCCESS || !drain()) {
                slot->stopped.store(true);
                return;
            }
            continue;
        }
        if (!carrier->MoreReadEvents(&progress)) { return; }
    }
}

void UrmaEndpoint::ApplyRemoteHello(const ParsedHello& remote) {
    _remote_recv_block_size = remote.buffer_size;
    _peer_socket_id = remote.socket_id;
}

// ============================================================================
// OnNewDataFromTcp: edge-triggered dispatcher.
// ============================================================================

static void TryReadOnTcpDuringUrmaEst(Socket* socket);

void UrmaEndpoint::OnNewDataFromTcp(Socket* m) {
    auto* tp = static_cast<UrmaTransport*>(m->_transport.get());
    if (!tp) {
        return;
    }
    // Access _urma_ep directly (OnNewDataFromTcp is a friend of UrmaTransport);
    // GetUrmaEp() CHECKs non-null which would crash on TCP-fallback sockets.
    UrmaEndpoint* ep = tp->_urma_ep;
    if (!ep) {
        // No URMA endpoint: pure TCP path.
        InputMessenger::OnNewMessages(m);
        return;
    }
    int progress = Socket::PROGRESS_INIT;
    while (true) {
        const State state =
            ep->_state.load(butil::memory_order_acquire);
        if (state == UNINIT) {
            if (!m->CreatedByConnect()) {
                // Server side: kick off the handshake bthread.
                if (!IsUrmaAvailable()) {
                    ep->_state = FALLBACK_TCP;
                    tp->_urma_state = UrmaTransport::URMA_OFF;
                    InputMessenger::OnNewMessages(m);
                    return;
                }
                SocketUniquePtr s;
                m->ReAddress(&s);
                ep->_state = S_HELLO_WAIT;
                bthread_t tid;
                bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
                bthread_attr_set_name(&attr, "UrmaServerHandshake");
                if (bthread_start_background(&tid, &attr,
                        ProcessHandshakeAtServer, ep) != 0) {
                    ep->_state = UNINIT;
                    LOG(FATAL) << "Fail to start UrmaServerHandshake bthread";
                } else {
                    s.release();
                }
            }
            // Client side: handled by ProcessHandshakeAtClient.
        } else if (state < ESTABLISHED) {
            // During handshake: wake the handshake bthread parked in ReadFromFd.
            ep->_read_butex->fetch_add(1, butil::memory_order_release);
            bthread::butex_wake(ep->_read_butex);
        } else if (state == FALLBACK_TCP) {
            InputMessenger::OnNewMessages(m);
            return;
        } else if (state == ESTABLISHED) {
            TryReadOnTcpDuringUrmaEst(m);
            return;
        }
        if (!m->MoreReadEvents(&progress)) {
            break;
        }
    }
}

inline void UrmaEndpoint::TryReadOnTcp() {
    // Handshake and epoll callbacks may run concurrently. Schedule through
    // Socket's event counter, never start a second TCP reader directly. Also
    // recheck EOF consumed as a handshake notification before ESTABLISHED.
    Socket::OnInputEvent(reinterpret_cast<void*>(_socket->id()), 0,
                         BTHREAD_ATTR_NORMAL);
}

static void TryReadOnTcpDuringUrmaEst(Socket* socket) {
    int progress = Socket::PROGRESS_INIT;
    while (true) {
        uint8_t byte = 0;
        const ssize_t nr = read(socket->fd(), &byte, 1);
        if (nr < 0) {
            if (errno != EAGAIN) {
                const int saved_errno = errno;
                socket->SetFailed(saved_errno, "Fail to read URMA TCP fd: %s",
                                  berror(saved_errno));
                return;
            }
            if (!socket->MoreReadEvents(&progress)) {
                return;
            }
        } else if (nr == 0) {
            socket->SetEOF();
            return;
        } else {
            socket->SetFailed(
                EPROTO, "Unexpected TCP data after URMA was established");
            return;
        }
    }
}

void UrmaEndpoint::FallbackToTcp(UrmaTransport* transport, bool process_tcp) {
    transport->_urma_state = UrmaTransport::URMA_OFF;
    _state.store(FALLBACK_TCP, butil::memory_order_release);
    DeallocateResources();
    if (process_tcp) {
        TryReadOnTcp();
    }
}

void UrmaEndpoint::FailHandshake(UrmaTransport* transport, int error,
                                 const char* reason) {
    LOG(ERROR) << "URMA handshake failed in state=" << GetStateStr()
               << " on " << _socket->description()
               << ": " << reason << ", error=" << error
               << " (" << berror(error) << ')';
    transport->_urma_state = UrmaTransport::URMA_OFF;
    _state.store(FAILED, butil::memory_order_release);
    DeallocateResources();
    auto* connect =
        static_cast<UrmaConnect*>(_socket->_app_connect.get());
    if (connect) {
        connect->_error = error;
    }
    _socket->SetFailed(error, "URMA handshake failed: %s", reason);
}

// ============================================================================
// Handshake state machines (client / server). Run in a background bthread.
// ============================================================================

void* UrmaEndpoint::ProcessHandshakeAtClient(void* arg) {
    auto* ep = static_cast<UrmaEndpoint*>(arg);
    SocketUniquePtr s(ep->_socket);
    auto* tp = static_cast<UrmaTransport*>(s->_transport.get());
    UrmaConnect::RunGuard guard(static_cast<UrmaConnect*>(s->_app_connect.get()));
    if (!IsUrmaAvailable()) {
        ep->FallbackToTcp(tp, true);
        return nullptr;
    }
    ep->_state = C_ALLOC_RES;
    if (ep->AllocateResources() < 0) {
        ep->FallbackToTcp(tp, true);
        return nullptr;
    }
    ep->_state = C_HELLO_SEND;
    std::unique_ptr<UrmaHandshake> hs(CreateClientHandshake(ep));
    ep->_handshake_version = hs->ProtocolVersion();
    if (hs->SendLocalHello() < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "send client hello");
        return nullptr;
    }
    ep->_state = C_HELLO_WAIT;
    ParsedHello remote;
    bool negotiated = false;
    if (hs->ReceiveAndParseRemoteHello(&remote, &negotiated) < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "read server hello");
        return nullptr;
    }
    if (!negotiated) {
        ep->FallbackToTcp(tp, true);
        return nullptr;
    }
    ep->ApplyRemoteHello(remote);
    ep->_state = C_IMPORT_PEER;
    if (ep->ImportPeer(remote) < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "import server resources");
        return nullptr;
    }
    ep->_state = C_ACK_SEND;
    uint32_t flags = HELLO_ACK_URMA_OK;
    uint32_t flags_be = butil::HostToNet32(flags);
    if (ep->WriteToFd(&flags_be, HELLO_ACK_LEN) < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "send client ack");
        return nullptr;
    }
    tp->_urma_state = UrmaTransport::URMA_ON;
    ep->_state = ESTABLISHED;
    ep->DispatchReceivedBytes(s, 0);
    ep->TryReadOnTcp();
    return nullptr;
}

void* UrmaEndpoint::ProcessHandshakeAtServer(void* arg) {
    auto* ep = static_cast<UrmaEndpoint*>(arg);
    SocketUniquePtr s(ep->_socket);
    auto* tp = static_cast<UrmaTransport*>(s->_transport.get());
    UrmaConnect::RunGuard guard(static_cast<UrmaConnect*>(s->_app_connect.get()));
    ep->_state = S_HELLO_WAIT;
    uint8_t magic[v2_wire::MAGIC_STR_LEN];
    if (ep->ReadFromFd(magic, v2_wire::MAGIC_STR_LEN) < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "read client magic");
        return nullptr;
    }
    std::unique_ptr<UrmaHandshake> hs(CreateServerHandshakeByMagic(ep, magic));
    if (!hs) {
        // Not an URMA peer: push the magic back and fall back to TCP.
        ep->PushBackToReadBuf(magic, v2_wire::MAGIC_STR_LEN);
        ep->FallbackToTcp(tp, true);
        return nullptr;
    }
    ep->_handshake_version = hs->ProtocolVersion();
    ParsedHello remote;
    bool negotiated = false;
    if (hs->ReceiveAndParseRemoteHello(&remote, &negotiated) < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "read client hello");
        return nullptr;
    }
    if (!negotiated) {
        ep->FailHandshake(tp, EPROTO, "invalid client hello");
        return nullptr;
    }
    ep->_state = S_ALLOC_RES;
    if (ep->AllocateResources() < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "allocate server resources");
        return nullptr;
    }
    ep->ApplyRemoteHello(remote);
    ep->_state = S_IMPORT_PEER;
    if (ep->ImportPeer(remote) < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "import client resources");
        return nullptr;
    }
    ep->_state = S_HELLO_SEND;
    if (hs->SendLocalHello() < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "send server hello");
        return nullptr;
    }
    ep->_state = S_ACK_WAIT;
    uint32_t flags_be = 0;
    if (ep->ReadFromFd(&flags_be, HELLO_ACK_LEN) < 0) {
        const int saved_errno = errno ? errno : EIO;
        ep->FailHandshake(tp, saved_errno, "read client ack");
        return nullptr;
    }
    uint32_t flags = butil::NetToHost32(flags_be);
    bool client_ack_ok = (flags & HELLO_ACK_URMA_OK) != 0;
    if (client_ack_ok) {
        if (tp->_urma_state.load(butil::memory_order_acquire) ==
            UrmaTransport::URMA_OFF) {
            // Protocol breakdown: client wants URMA but we already fell back.
            ep->FailHandshake(tp, EPROTO, "client ack mismatch");
            return nullptr;
        }
        tp->_urma_state = UrmaTransport::URMA_ON;
        ep->_state = ESTABLISHED;
        ep->DispatchReceivedBytes(s, 0);
        ep->TryReadOnTcp();
    } else {
        ep->FallbackToTcp(tp, true);
    }
    return nullptr;
}

// ============================================================================
// UrmaConnect: drives the client handshake bthread.
// ============================================================================

void UrmaConnect::StartConnect(const Socket* socket,
                               void (*done)(int, void*), void* data) {
    SocketUniquePtr s;
    if (Socket::Address(socket->id(), &s) != 0) {
        return;
    }
    _done = done;
    _data = data;
    _error = 0;
    auto* tp = static_cast<UrmaTransport*>(socket->_transport.get());
    if (!tp) {
        Run();
        return;
    }
    if (!tp->_urma_ep || !IsUrmaAvailable()) {
        // Fall back to TCP immediately.
        if (tp->_urma_ep) {
            tp->_urma_ep->_state = UrmaEndpoint::FALLBACK_TCP;
        }
        tp->_urma_state = UrmaTransport::URMA_OFF;
        Run();
        return;
    }
    bthread_t tid;
    bthread_attr_t attr = BTHREAD_ATTR_NORMAL;
    bthread_attr_set_name(&attr, "UrmaClientHandshake");
    if (bthread_start_background(&tid, &attr,
            UrmaEndpoint::ProcessHandshakeAtClient,
            tp->_urma_ep) != 0) {
        tp->_urma_ep->_state = UrmaEndpoint::FALLBACK_TCP;
        tp->_urma_state = UrmaTransport::URMA_OFF;
        Run();
    } else {
        // ProcessHandshakeAtClient adopts this reference in its
        // SocketUniquePtr constructor.
        s.release();
    }
}

void UrmaConnect::StopConnect(Socket*) {}

void UrmaConnect::Run() {
    if (_done) {
        auto cb = _done;
        _done = nullptr;
        cb(_error, _data);
    }
}

// ============================================================================
// Debug / polling-mode stubs.
// ============================================================================

std::string UrmaEndpoint::GetStateStr() const {
    switch (_state.load(butil::memory_order_acquire)) {
    case UNINIT:        return "UNINIT";
    case C_ALLOC_RES:   return "C_ALLOC_RES";
    case C_HELLO_SEND:  return "C_HELLO_SEND";
    case C_HELLO_WAIT:  return "C_HELLO_WAIT";
    case C_IMPORT_PEER: return "C_IMPORT_PEER";
    case C_ACK_SEND:    return "C_ACK_SEND";
    case S_HELLO_WAIT:  return "S_HELLO_WAIT";
    case S_ALLOC_RES:   return "S_ALLOC_RES";
    case S_IMPORT_PEER: return "S_IMPORT_PEER";
    case S_HELLO_SEND:  return "S_HELLO_SEND";
    case S_ACK_WAIT:    return "S_ACK_WAIT";
    case ESTABLISHED:   return "ESTABLISHED";
    case FALLBACK_TCP:  return "FALLBACK_TCP";
    case FAILED:        return "FAILED";
    }
    return "UNKNOWN";
}

void UrmaEndpoint::DebugInfo(std::ostream& os, butil::StringPiece) const {
    os << "state=" << GetStateStr()
       << " self_socket_id=" << _self_socket_id
       << " peer_socket_id=" << _peer_socket_id
       << " remote_recv_block_size=" << _remote_recv_block_size
       << " handshake_version=" << _handshake_version;
}

int UrmaEndpoint::PollingModeInitialize(bthread_tag_t,
        std::function<void()>, std::function<void()>, std::function<void()>) {
    if (FLAGS_urma_use_polling) { errno = ENOTSUP; return -1; }
    return 0;
}
void UrmaEndpoint::PollingModeRelease(bthread_tag_t) {}

int UrmaEndpoint::GlobalInitialize() {
    if (g_urma_pool) { return 0; }
    if (!GetUrmaContext()) { return 0; }
    if (FLAGS_urma_use_polling || IsUrmaBondingDevice()) {
        errno = ENOTSUP;
        return -1;
    }
    g_urma_pool = CreateUrmaResourcePool();
    if (!g_urma_pool) { errno = ENOMEM; return -1; }
    UrmaResourcePool::EventAdapter events;
    events.attach = AttachSlotSocket;
    events.detach = DetachSlotSocket;
    return g_urma_pool->Initialize(GetUrmaContext(), events);
}
void UrmaEndpoint::GlobalRelease() {
    if (g_urma_pool && g_urma_pool->Shutdown()) {
        delete g_urma_pool;
        g_urma_pool = nullptr;
    }
}

}  // namespace urma
}  // namespace brpc

#endif  // BRPC_WITH_URMA
