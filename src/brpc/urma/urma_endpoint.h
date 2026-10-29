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

#ifndef BRPC_URMA_ENDPOINT_H
#define BRPC_URMA_ENDPOINT_H

#include <cstdint>
#include <functional>
#include <ostream>
#include <map>
#include <memory>

#include "butil/atomicops.h"
#include "butil/iobuf.h"
#include "butil/macros.h"
#include "butil/synchronization/lock.h"
#include "bthread/types.h"

#include "brpc/socket.h"

#if BRPC_WITH_URMA

#include "urma_api.h"
#include "urma_types.h"
#include "brpc/urma/urma_handshake.h"
#include "brpc/urma/urma_resource_pool.h"
#include "brpc/urma/urma_handshake.pb.h"

namespace brpc {

class UrmaTransport;

namespace urma {

class UrmaHandshake;
struct ParsedHello;

// Callback-owned, bounded to one message across all logical connections.
// Declare before the CQ lock so destruction always happens after unlocking.
struct UrmaDeferredMessage {
    UrmaDeferredMessage() = default;
    InputMessageBase* message{nullptr};
    bool event_released{false};
    ~UrmaDeferredMessage() noexcept(false);
private:
    DISALLOW_COPY_AND_ASSIGN(UrmaDeferredMessage);
};

DECLARE_bool(urma_inline_last);
DECLARE_bool(urma_use_polling);
DECLARE_int32(urma_poller_num);
DECLARE_bool(urma_disable_bthread);

// Per-connection application-level connect object. Returned by
// UrmaTransport::Connect(); its StartConnect spawns the client-side handshake
// bthread that drives the URMA negotiation over the already-connected TCP fd.
class UrmaConnect : public AppConnect {
    friend class UrmaEndpoint;
public:
    void StartConnect(const Socket* socket,
                      void (*done)(int err, void* data), void* data) override;
    void StopConnect(Socket*) override;

    struct RunGuard {
        explicit RunGuard(UrmaConnect* rc) : this_rc(rc) {}
        ~RunGuard() { if (this_rc) this_rc->Run(); }
        UrmaConnect* this_rc;
    };

private:
    void Run();
    void (*_done)(int, void*){nullptr};
    void* _data{nullptr};
    int _error{0};
};

// One protocol endpoint per Socket; hardware queues belong to the global pool.
class BAIDU_CACHELINE_ALIGNMENT UrmaEndpoint : public SocketUser {
    friend class UrmaConnect;
    friend class Socket;
    friend class ::brpc::UrmaTransport;
    friend class UrmaHandshakeClientV2;
    friend class UrmaHandshakeServerV2;
    friend class UrmaHandshakeClientV3;
    friend class UrmaHandshakeServerV3;
    friend int DrainBytes(UrmaEndpoint*, size_t);
    friend int ReadBodyAndNegotiate(UrmaEndpoint*, ParsedHello*, bool*);

public:
    explicit UrmaEndpoint(Socket* s);
    ~UrmaEndpoint() override;

    // ---- Global initialization / release ----
    // Create the process-wide JFS/JFR pool. Called once at GlobalUrmaInitializeOrDie.
    static int GlobalInitialize();
    static void GlobalRelease();

    // ---- Per-connection lifecycle ----
    // Reset the endpoint back to UNINIT for reuse.
    void Reset();

    // ---- Data path (called by UrmaTransport) ----
    // Cut data from the IOBuf list and post it via URMA SEND.
    // Returns bytes sent, or -1 (errno set; EAGAIN when the shared SQ pool is full).
    ssize_t CutFromIOBufList(butil::IOBuf** data, size_t ndata);

    // Whether the endpoint can post more sends (the shared pool has SQ capacity).
    bool IsWritable() const;

    // For debug: dump endpoint state to @os.
    void DebugInfo(std::ostream& os,
                   butil::StringPiece connector = "\n") const;

    // Edge-triggered callback installed on the TCP socket when the transport
    // is URMA. Dispatches on _state (see the .cpp for the full state machine):
    //   UNINIT (server)  -> start the server handshake bthread
    //   < ESTABLISHED    -> wake the handshake bthread parked in ReadFromFd
    //   FALLBACK_TCP     -> hand off to InputMessenger::OnNewMessages
    //   ESTABLISHED      -> drain stray TCP bytes
    static void OnNewDataFromTcp(Socket* m);

    // Event adapter for shared queue slots.
    static void PollIoJfc(Socket*);
    static int AttachSlotSocket(UrmaIoSlot*);
    static void DetachSlotSocket(UrmaIoSlot*);
    static void HandleTxCompletion(UrmaJfsSlot*, const urma_cr_t&);
    static void HandleRxCompletion(UrmaJfrSlot*, const urma_cr_t&,
                                   UrmaDeferredMessage* = nullptr);

    // Compatibility entry points; polling mode currently returns ENOTSUP.
    static int PollingModeInitialize(bthread_tag_t tag,
                                     std::function<void()> callback,
                                     std::function<void()> init_fn,
                                     std::function<void()> release_fn);
    static void PollingModeRelease(bthread_tag_t tag);

    // ---- Handshake IO helpers (also used by urma_handshake.cpp) ----
    // Read at most @len bytes from the TCP fd into @data; waits on _read_butex
    // on EAGAIN. Returns 0 on success, -1 on IO error (errno set).
    int ReadFromFd(void* data, size_t len);
    // Push @len bytes back into the TCP input processor so it can re-parse
    // them (used on fallback when the magic is not "URMA").
    void PushBackToReadBuf(const void* data, size_t len);
    // Write at most @len bytes from @data to the TCP fd; waits on
    // _epollout_butex on EAGAIN. Returns 0 on success, -1 on IO error.
    int WriteToFd(void* data, size_t len);
    // Write an IOBuf to the TCP fd (used by v3 protobuf handshake).
    int WriteToFd(butil::IOBuf& data);

    // ---- Hello builders/parsers (used by urma_handshake.cpp) ----
    // Fill the v2 binary HelloMessage with this endpoint's local params.
    void FillLocalHelloV2(v2_wire::HelloMessage* out) const;
    // Fill the v3 protobuf UrmaHello with this endpoint's local params.
    void FillLocalHelloV3(UrmaHello* out) const;
    // Write "URM3" + 4B big-endian pb_size + protobuf bytes.
    int WriteHelloV3(const UrmaHello& msg);
    // Read pb_size (4B) + protobuf bytes; parse; validate. Sets *negotiated
    // to false (returns 0) if the message is invalid.
    int ReadAndParseHelloV3(ParsedHello* out, bool* negotiated);

    // Apply the peer's negotiated parameters (receive size and destination SocketId).
    void ApplyRemoteHello(const ParsedHello& remote);

private:
    enum State {
        UNINIT = 0x0,
        C_ALLOC_RES   = 0x1,  // client: select shared JFR
        C_HELLO_SEND  = 0x2,
        C_HELLO_WAIT  = 0x3,
        C_IMPORT_PEER = 0x4,  // urma_import_seg then urma_import_jfr
        C_ACK_SEND    = 0x5,
        S_HELLO_WAIT  = 0x11,
        S_ALLOC_RES   = 0x12,
        S_IMPORT_PEER = 0x13,
        S_HELLO_SEND  = 0x14,
        S_ACK_WAIT    = 0x15,
        ESTABLISHED   = 0x100,
        FALLBACK_TCP  = 0x200,
        FAILED        = 0x300
    };

    // Process handshake at the client / server (run in a background bthread).
    static void* ProcessHandshakeAtClient(void* arg);
    static void* ProcessHandshakeAtServer(void* arg);

    // Allocate only connection metadata; shared queues survive disconnect.
    int AllocateResources();
    void DeallocateResources();

    // Import the peer's shared JFR and buffer-pool segment. CRITICAL:
    // urma_import_seg is called BEFORE urma_import_jfr, otherwise the kernel
    // does not establish the transport-path routing for the remote EID and
    // the first SEND is rejected with URMA_CR_RNR_RETRY_CNT_EXC_ERR.
    // Returns 0 on success.
    int ImportPeer(const ParsedHello& peer);

    void ReorderAndDispatch(SocketUniquePtr&, uint16_t seq, butil::IOBuf data,
                            UrmaDeferredMessage* = nullptr);
    void DispatchReceivedBytesLocked(SocketUniquePtr&, ssize_t bytes,
                                     UrmaDeferredMessage* = nullptr);

    // Queue received bytes for InputMessenger. CQ receive completions can
    // arrive while the server is still waiting for the final TCP handshake
    // ACK. Keep those bytes in the URMA input processor and dispatch them only
    // after ESTABLISHED, matching the connection-ready boundary seen by user
    // code.
    void DispatchReceivedBytes(SocketUniquePtr& s, ssize_t bytes,
                               UrmaDeferredMessage* = nullptr);

    inline void TryReadOnTcp();
    void FallbackToTcp(UrmaTransport* transport, bool process_tcp);
    void FailHandshake(UrmaTransport* transport, int error,
                       const char* reason);

    // Construct a ParsedHello from local state (used by SendLocalHello).
    void MakeLocalParsedHello(ParsedHello* out) const;

    std::string GetStateStr() const;

    // Not owned.
    Socket* _socket;

    // Handshake state.
    butil::atomic<State> _state{UNINIT};
    int _handshake_version{0};  // 0 = unnegotiated; 2 = v2; 3 = v3
    butil::atomic<int64_t> _pending_received_bytes{0};
    butil::Mutex _dispatch_mutex;
    // Only valid during ProcessNewMessage, protected by _dispatch_mutex.
    UrmaDeferredMessage* _deferred_message{nullptr};

    std::shared_ptr<UrmaConnResource> _conn_res;
    SocketId _self_socket_id{INVALID_SOCKET_ID};
    SocketId _peer_socket_id{INVALID_SOCKET_ID};
    // Selected on the first send; a full queue may temporarily spill to peers.
    uint32_t _preferred_jfs{UINT32_MAX};
    uint16_t _next_send_seq{0};
    uint16_t _deliver_base{0};
    butil::Mutex _reorder_mutex;
    std::map<uint16_t, butil::IOBuf> _reorder_queue;
    InputMessengerProcessor _input_processor;
    uint32_t _remote_recv_block_size{0};

    // Butex for waking the handshake bthread parked in ReadFromFd.
    butil::atomic<int>* _read_butex{nullptr};

    DISALLOW_COPY_AND_ASSIGN(UrmaEndpoint);
};

}  // namespace urma
}  // namespace brpc

#else  // BRPC_WITH_URMA

namespace brpc {
namespace urma {
class UrmaEndpoint {};
}  // namespace urma
}  // namespace brpc

#endif  // BRPC_WITH_URMA

#endif  // BRPC_URMA_ENDPOINT_H
