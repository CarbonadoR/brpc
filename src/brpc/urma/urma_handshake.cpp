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

#include "brpc/urma/urma_handshake.h"

#if BRPC_WITH_URMA

#include <algorithm>
#include <cstring>
#include <limits>

#include <gflags/gflags.h>

#include "butil/atomicops.h"
#include "butil/iobuf.h"          // IOBuf, IOPortal, IOBufAsZeroCopy*
#include "butil/logging.h"
#include "butil/sys_byteorder.h"

#include "brpc/socket.h"
#include "brpc/urma/urma_endpoint.h"
#include "brpc/urma/urma_helper.h"
#include "brpc/urma/urma_handshake.pb.h"
#include "brpc/urma_transport.h"

namespace brpc {
namespace urma {

DEFINE_int32(urma_client_handshake_version, 2,
              "Client handshake version: 2 = binary, 3 = protobuf");

// ============================================================================
// v2 binary HelloMessage.
// On-wire layout (network byte order, tightly packed body of 90 bytes):
//
//   offset  field            size
//      0    msg_len          2B    (full packet length incl. magic)
//      2    hello_ver        2B
//      4    impl_ver         2B
//      6    buffer_size      4B
//     10    recv_buffer_cnt  4B
//     14    jfr_id         4B
//     18    eid             16B    (raw, no swap)
//     34    uasid            4B
//     38    tp_type          1B
//     39    pad              3B
//     42    seg_eid         16B    (raw)
//     58    seg_uasid        4B
//     62    seg_va           8B
//     70    seg_len          8B
//     78    seg_token_id     4B
//     82    socket_id        8B
//   total = 90 bytes body.
//
// Full packet = magic "URMA" (4B) + body (90B) = 94 bytes.
// ============================================================================

namespace v2_wire {

void HelloMessage::Serialize(void* buf) const {
    uint8_t* p = static_cast<uint8_t*>(buf);
    auto write16 = [&p](uint16_t value) {
        value = butil::HostToNet16(value);
        std::memcpy(p, &value, sizeof(value));
        p += sizeof(value);
    };
    auto write32 = [&p](uint32_t value) {
        value = butil::HostToNet32(value);
        std::memcpy(p, &value, sizeof(value));
        p += sizeof(value);
    };
    auto write64 = [&p](uint64_t value) {
        value = butil::HostToNet64(value);
        std::memcpy(p, &value, sizeof(value));
        p += sizeof(value);
    };

    write16(msg_len);
    write16(hello_ver);
    write16(impl_ver);
    write32(buffer_size);
    write32(recv_buffer_cnt);
    write32(jfr_id);
    std::memcpy(p, eid, sizeof(eid));
    p += sizeof(eid);
    write32(uasid);
    *p++ = tp_type;
    std::memset(p, 0, sizeof(pad));
    p += sizeof(pad);
    std::memcpy(p, seg_eid, sizeof(seg_eid));
    p += sizeof(seg_eid);
    write32(seg_uasid);
    write64(seg_va);
    write64(seg_len);
    write32(seg_token_id);
    write64(socket_id);
}

void HelloMessage::Deserialize(const void* buf) {
    const uint8_t* p = static_cast<const uint8_t*>(buf);
    auto read16 = [&p]() {
        uint16_t value;
        std::memcpy(&value, p, sizeof(value));
        p += sizeof(value);
        return butil::NetToHost16(value);
    };
    auto read32 = [&p]() {
        uint32_t value;
        std::memcpy(&value, p, sizeof(value));
        p += sizeof(value);
        return butil::NetToHost32(value);
    };
    auto read64 = [&p]() {
        uint64_t value;
        std::memcpy(&value, p, sizeof(value));
        p += sizeof(value);
        return butil::NetToHost64(value);
    };

    msg_len = read16();
    hello_ver = read16();
    impl_ver = read16();
    buffer_size = read32();
    recv_buffer_cnt = read32();
    jfr_id = read32();
    std::memcpy(eid, p, sizeof(eid));
    p += sizeof(eid);
    uasid = read32();
    tp_type = *p++;
    p += sizeof(pad);
    std::memcpy(seg_eid, p, sizeof(seg_eid));
    p += sizeof(seg_eid);
    seg_uasid = read32();
    seg_va = read64();
    seg_len = read64();
    seg_token_id = read32();
    socket_id = read64();
}

}  // namespace v2_wire

// ============================================================================
// Shared helpers.
// ============================================================================

namespace {

constexpr uint32_t MIN_BUFFER_SIZE = 1024 - sizeof(butil::IOBuf::Block);
// Shared RQ depth; no per-connection credit protocol in this version.
constexpr uint32_t MIN_BUFFER_CNT = 1;
constexpr uint32_t MAX_BUFFER_CNT = 65535;
constexpr uint32_t MAX_V3_PB_SIZE = 4096;

}  // namespace

bool ValidHello(const ParsedHello& h) {
    if (h.buffer_size < MIN_BUFFER_SIZE) {
        return false;
    }
    if (h.recv_buffer_cnt < MIN_BUFFER_CNT || h.recv_buffer_cnt > MAX_BUFFER_CNT) {
        return false;
    }
    if (h.jfr_id == 0 || h.socket_id == INVALID_SOCKET_ID) {
        return false;
    }
    if (h.tp_type != static_cast<uint8_t>(URMA_CTP)) {
        return false;
    }
    // The provider consumes the advertised VA/length as a single remote
    // segment. Reject a wrapped end address before it reaches the import
    // API, which could otherwise turn a malformed hello into an invalid
    // segment range.
    if (h.seg_len == 0 || h.seg_va == 0 ||
        h.seg_len > std::numeric_limits<uint64_t>::max() - h.seg_va) {
        return false;
    }
    return true;
}

// File-local (not in the anonymous namespace so it can be friend-declared
// from urma_endpoint.h's UrmaEndpoint). Reads the body following the magic
// and translates it into ParsedHello.
int ReadBodyAndNegotiate(UrmaEndpoint* ep, ParsedHello* out, bool* negotiated) {
    *negotiated = false;
    uint8_t body[v2_wire::HELLO_BODY_LEN];
    // Inspect the fixed prefix before reading the extended body. Waiting for
    // 90 bytes from an old 82-byte hello deadlocks both handshake peers.
    if (ep->ReadFromFd(body, 6) < 0) { return -1; }
    uint16_t length, version, implementation;
    std::memcpy(&length, body, 2);
    std::memcpy(&version, body + 2, 2);
    std::memcpy(&implementation, body + 4, 2);
    length = butil::NetToHost16(length);
    if (length < v2_wire::HELLO_MSG_LEN_MIN ||
        length > v2_wire::HELLO_MSG_LEN_MAX ||
        butil::NetToHost16(version) != v2_wire::HELLO_V2_VERSION ||
        butil::NetToHost16(implementation) != v2_wire::IMPL_V2_VERSION) {
        errno = EPROTO;
        return -1;
    }
    if (ep->ReadFromFd(body + 6, v2_wire::HELLO_BODY_LEN - 6) < 0) { return -1; }
    v2_wire::HelloMessage m;
    m.Deserialize(body);
    if (m.msg_len < v2_wire::HELLO_MSG_LEN_MIN ||
        m.msg_len > v2_wire::HELLO_MSG_LEN_MAX ||
        m.hello_ver != v2_wire::HELLO_V2_VERSION ||
        m.impl_ver != v2_wire::IMPL_V2_VERSION) { return 0; }
    ParsedHello p;
    p.buffer_size = m.buffer_size;
    p.recv_buffer_cnt = m.recv_buffer_cnt;
    p.jfr_id = m.jfr_id;
    p.socket_id = m.socket_id;
    std::memcpy(p.eid, m.eid, 16);
    p.uasid = m.uasid;
    p.tp_type = m.tp_type;
    std::memcpy(p.seg_eid, m.seg_eid, 16);
    p.seg_uasid = m.seg_uasid;
    p.seg_va = m.seg_va;
    p.seg_len = m.seg_len;
    p.seg_token_id = m.seg_token_id;
    if (!ValidHello(p)) {
        return 0;
    }
    // Drain trailing bytes if msg_len advertises more than the fixed body.
    if (m.msg_len > v2_wire::HELLO_PACKET_LEN) {
        if (DrainBytes(ep, m.msg_len - v2_wire::HELLO_PACKET_LEN) < 0) {
            return -1;
        }
    }
    *out = p;
    *negotiated = true;
    return 0;
}

int DrainBytes(UrmaEndpoint* ep, size_t n) {
    char buf[4096];
    while (n > 0) {
        size_t want = std::min(n, sizeof(buf));
        if (ep->ReadFromFd(buf, want) < 0) {
            return -1;
        }
        n -= want;
    }
    return 0;
}

// ============================================================================
// v2 client / server.
// ============================================================================

int UrmaHandshakeClientV2::SendLocalHello() {
    v2_wire::HelloMessage m;
    _ep->FillLocalHelloV2(&m);
    uint8_t packet[v2_wire::HELLO_PACKET_LEN];
    std::memcpy(packet, "URMA", 4);
    m.Serialize(packet + 4);
    return _ep->WriteToFd(packet, v2_wire::HELLO_PACKET_LEN);
}

int UrmaHandshakeClientV2::ReceiveAndParseRemoteHello(ParsedHello* out,
                                                      bool* negotiated) {
    *negotiated = false;
    uint8_t magic[v2_wire::MAGIC_STR_LEN];
    if (_ep->ReadFromFd(magic, v2_wire::MAGIC_STR_LEN) < 0) {
        return -1;
    }
    if (std::memcmp(magic, "URMA", 4) != 0) {
        // Peer is not URMA-capable; push the magic back so the TCP input
        // messenger can re-parse it.
        _ep->PushBackToReadBuf(magic, v2_wire::MAGIC_STR_LEN);
        return 0;
    }
    return ReadBodyAndNegotiate(_ep, out, negotiated);
}

int UrmaHandshakeServerV2::ReceiveAndParseRemoteHello(ParsedHello* out,
                                                       bool* negotiated) {
    return ReadBodyAndNegotiate(_ep, out, negotiated);
}

int UrmaHandshakeServerV2::SendLocalHello() {
    v2_wire::HelloMessage m;
    _ep->FillLocalHelloV2(&m);
    auto* tp = static_cast<UrmaTransport*>(_ep->_socket->_transport.get());
    if (tp->_urma_state.load(butil::memory_order_acquire) ==
        UrmaTransport::URMA_OFF) {
        // Tell the client we are not URMA-capable: zero the version fields so
        // the client's version check fails and it falls back to TCP.
        m.hello_ver = 0;
        m.impl_ver = 0;
        m.jfr_id = 0;
        m.buffer_size = 0;
    }
    uint8_t packet[v2_wire::HELLO_PACKET_LEN];
    std::memcpy(packet, "URMA", 4);
    m.Serialize(packet + 4);
    return _ep->WriteToFd(packet, v2_wire::HELLO_PACKET_LEN);
}

// ============================================================================
// v3 protobuf ("URM3"). The protobuf (de)serialization lives on the endpoint
// because it touches UrmaEndpoint private state; the classes here just call
// FillLocalHelloV3 / WriteHelloV3 / ReadAndParseHelloV3.
// ============================================================================

int UrmaHandshakeClientV3::SendLocalHello() {
    UrmaHello msg;
    _ep->FillLocalHelloV3(&msg);
    return _ep->WriteHelloV3(msg);
}

int UrmaHandshakeClientV3::ReceiveAndParseRemoteHello(ParsedHello* out,
                                                      bool* negotiated) {
    *negotiated = false;
    uint8_t magic[v2_wire::MAGIC_STR_LEN];
    if (_ep->ReadFromFd(magic, v2_wire::MAGIC_STR_LEN) < 0) {
        return -1;
    }
    if (std::memcmp(magic, "URM3", 4) != 0) {
        _ep->PushBackToReadBuf(magic, v2_wire::MAGIC_STR_LEN);
        return 0;
    }
    return _ep->ReadAndParseHelloV3(out, negotiated);
}

int UrmaHandshakeServerV3::ReceiveAndParseRemoteHello(ParsedHello* out,
                                                      bool* negotiated) {
    return _ep->ReadAndParseHelloV3(out, negotiated);
}

int UrmaHandshakeServerV3::SendLocalHello() {
    UrmaHello msg;
    _ep->FillLocalHelloV3(&msg);
    // v3 has no zero-out path; the client rejects jfr_id==0 via ValidHello.
    return _ep->WriteHelloV3(msg);
}

// ============================================================================
// Factories.
// ============================================================================

UrmaHandshake* CreateClientHandshake(UrmaEndpoint* ep) {
    switch (FLAGS_urma_client_handshake_version) {
    case 3: return new UrmaHandshakeClientV3(ep);
    case 2:
    default: return new UrmaHandshakeClientV2(ep);
    }
}

UrmaHandshake* CreateServerHandshakeByMagic(UrmaEndpoint* ep,
                                             const uint8_t magic[v2_wire::MAGIC_STR_LEN]) {
    if (std::memcmp(magic, "URMA", 4) == 0) {
        return new UrmaHandshakeServerV2(ep);
    }
    if (std::memcmp(magic, "URM3", 4) == 0) {
        return new UrmaHandshakeServerV3(ep);
    }
    return nullptr;
}

}  // namespace urma
}  // namespace brpc

#endif  // BRPC_WITH_URMA
