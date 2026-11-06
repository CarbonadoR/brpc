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

#include <cstring>
#include <limits>
#include <gtest/gtest.h>
#include <gflags/gflags.h>

#if BRPC_WITH_URMA
#include "butil/atomicops.h"
#include "butil/sys_byteorder.h"
#include "urma_api.h"
#include "brpc/urma/urma_handshake.h"
#include "brpc/urma/urma_handshake.pb.h"
#include "brpc/urma/urma_helper.h"
#include "brpc/urma/urma_endpoint.h"
#include "brpc/urma_transport.h"
#include "urma_types.h"

using namespace brpc;

namespace brpc {
namespace urma {

DECLARE_int32(urma_client_handshake_version);
extern bool g_skip_urma_init;
extern butil::atomic<bool> g_urma_available;

}  // namespace urma
}  // namespace brpc

// ---------------------------------------------------------------------------
// v2 binary HelloMessage: serialize + deserialize round-trips.
// ---------------------------------------------------------------------------
TEST(UrmaHandshakeTest, v2_serialize_deserialize_roundtrip) {
    urma::v2_wire::HelloMessage m;
    m.msg_len = urma::v2_wire::HELLO_PACKET_LEN;
    m.hello_ver = urma::v2_wire::HELLO_V2_VERSION;
    m.impl_ver = urma::v2_wire::IMPL_V2_VERSION;
    m.buffer_size = 8192;
    m.recv_buffer_cnt = 127;
    m.jfr_id = 0x12345678;
    m.socket_id = 0x123456789abcdef0ULL;
    for (int i = 0; i < 16; ++i) {
        m.eid[i] = static_cast<uint8_t>(i + 1);
    }
    m.uasid = 0xdeadbeef;
    m.tp_type = 1;  // URMA_CTP
    for (int i = 0; i < 16; ++i) {
        m.seg_eid[i] = static_cast<uint8_t>(16 - i);
    }
    m.seg_uasid = 0xcafebabe;
    m.seg_va = 0x1122334455667788ULL;
    m.seg_len = 1ULL << 20;
    m.seg_token_id = 0x42424242;

    uint8_t buf[urma::v2_wire::HELLO_BODY_LEN];
    m.Serialize(buf);

    urma::v2_wire::HelloMessage m2;
    m2.Deserialize(buf);
    EXPECT_EQ(m.msg_len, m2.msg_len);
    EXPECT_EQ(m.hello_ver, m2.hello_ver);
    EXPECT_EQ(m.impl_ver, m2.impl_ver);
    EXPECT_EQ(m.buffer_size, m2.buffer_size);
    EXPECT_EQ(m.recv_buffer_cnt, m2.recv_buffer_cnt);
    EXPECT_EQ(m.jfr_id, m2.jfr_id);
    EXPECT_EQ(m.socket_id, m2.socket_id);
    EXPECT_EQ(0, memcmp(m.eid, m2.eid, 16));
    EXPECT_EQ(m.uasid, m2.uasid);
    EXPECT_EQ(m.tp_type, m2.tp_type);
    EXPECT_EQ(0, memcmp(m.seg_eid, m2.seg_eid, 16));
    EXPECT_EQ(m.seg_uasid, m2.seg_uasid);
    EXPECT_EQ(m.seg_va, m2.seg_va);
    EXPECT_EQ(m.seg_len, m2.seg_len);
    EXPECT_EQ(m.seg_token_id, m2.seg_token_id);
}

// ---------------------------------------------------------------------------
// v2 packet on the wire: "URMA" magic + body.
// ---------------------------------------------------------------------------
TEST(UrmaHandshakeTest, v2_packet_magic_is_urma) {
    EXPECT_EQ(4u, urma::v2_wire::MAGIC_STR_LEN);
    char magic[4] = {'U', 'R', 'M', 'A'};
    EXPECT_EQ(0, memcmp(magic, "URMA", 4));
    EXPECT_EQ(4u + 90u, urma::v2_wire::HELLO_PACKET_LEN);
}

// ---------------------------------------------------------------------------
// v3 protobuf UrmaHello: serialize + parse round-trips.
// ---------------------------------------------------------------------------
TEST(UrmaHandshakeTest, v3_protobuf_roundtrip) {
    urma::UrmaHello msg;
    msg.set_buffer_size(8192);
    msg.set_recv_buffer_cnt(127);
    msg.set_jfr_id(0x12345678);
    msg.set_socket_id(0x123456789abcdef0ULL);
    msg.set_impl_version(2);
    uint8_t eid[16];
    for (int i = 0; i < 16; ++i) {
        eid[i] = static_cast<uint8_t>(i + 1);
    }
    msg.set_eid(eid, 16);
    msg.set_uasid(0xdeadbeef);
    msg.set_tp_type(1);
    uint8_t seg_eid[16];
    for (int i = 0; i < 16; ++i) {
        seg_eid[i] = static_cast<uint8_t>(16 - i);
    }
    msg.set_seg_eid(seg_eid, 16);
    msg.set_seg_uasid(0xcafebabe);
    msg.set_seg_va(0x1122334455667788ULL);
    msg.set_seg_len(1ULL << 20);
    msg.set_seg_token_id(0x42424242);

    std::string body;
    ASSERT_TRUE(msg.SerializeToString(&body));
    urma::UrmaHello msg2;
    ASSERT_TRUE(msg2.ParseFromString(body));
    EXPECT_EQ(msg.buffer_size(), msg2.buffer_size());
    EXPECT_EQ(msg.recv_buffer_cnt(), msg2.recv_buffer_cnt());
    EXPECT_EQ(msg.jfr_id(), msg2.jfr_id());
    EXPECT_EQ(msg.socket_id(), msg2.socket_id());
    EXPECT_EQ(16, msg2.eid().size());
    EXPECT_EQ(0, memcmp(msg.eid().data(), msg2.eid().data(), 16));
    EXPECT_EQ(msg.uasid(), msg2.uasid());
    EXPECT_EQ(msg.tp_type(), msg2.tp_type());
    EXPECT_EQ(16, msg2.seg_eid().size());
    EXPECT_EQ(msg.seg_uasid(), msg2.seg_uasid());
    EXPECT_EQ(msg.seg_va(), msg2.seg_va());
    EXPECT_EQ(msg.seg_len(), msg2.seg_len());
    EXPECT_EQ(msg.seg_token_id(), msg2.seg_token_id());
}

// ---------------------------------------------------------------------------
// CreateServerHandshakeByMagic dispatches on the magic bytes.
// ---------------------------------------------------------------------------
TEST(UrmaHandshakeTest, server_handshake_factory_dispatches_on_magic) {
    // We cannot fully exercise the server handshake without a real socket +
    // endpoint, but we can verify the factory returns the right protocol
    // version for each magic, and nullptr for an unknown magic.
    uint8_t magic_v2[4] = {'U', 'R', 'M', 'A'};
    uint8_t magic_v3[4] = {'U', 'R', 'M', '3'};
    uint8_t magic_bad[4] = {'P', 'R', 'P', 'C'};

    // v2 magic -> protocol version 2
    urma::UrmaHandshake* hs2 =
        urma::CreateServerHandshakeByMagic(nullptr, magic_v2);
    // Note: the factory dereferences the endpoint only inside SendLocalHello /
    // ReceiveAndParseRemoteHello; passing nullptr is safe for the version query.
    // (We delete immediately to avoid touching the endpoint.)
    if (hs2) {
        EXPECT_EQ(2, hs2->ProtocolVersion());
        delete hs2;
    }
    // v3 magic -> protocol version 3
    urma::UrmaHandshake* hs3 =
        urma::CreateServerHandshakeByMagic(nullptr, magic_v3);
    if (hs3) {
        EXPECT_EQ(3, hs3->ProtocolVersion());
        delete hs3;
    }
    // unknown magic -> nullptr (caller falls back to TCP)
    urma::UrmaHandshake* hsb =
        urma::CreateServerHandshakeByMagic(nullptr, magic_bad);
    EXPECT_EQ(nullptr, hsb);
}

// ---------------------------------------------------------------------------
// CreateClientHandshake picks the version from the gflag.
// ---------------------------------------------------------------------------
TEST(UrmaHandshakeTest, client_handshake_factory_respects_flag) {
    GFLAGS_NAMESPACE::FlagSaver flag_saver;

    urma::FLAGS_urma_client_handshake_version = 2;
    urma::UrmaHandshake* hs2 = urma::CreateClientHandshake(nullptr);
    if (hs2) {
        EXPECT_EQ(2, hs2->ProtocolVersion());
        delete hs2;
    }

    urma::FLAGS_urma_client_handshake_version = 3;
    urma::UrmaHandshake* hs3 = urma::CreateClientHandshake(nullptr);
    if (hs3) {
        EXPECT_EQ(3, hs3->ProtocolVersion());
        delete hs3;
    }
}

// ---------------------------------------------------------------------------
// 4-byte ACK: HELLO_ACK_URMA_OK bit.
// ---------------------------------------------------------------------------
TEST(UrmaHandshakeTest, ack_bit_is_urma_ok) {
    // The ACK is a 4-byte big-endian flags word; bit 0 means "I want URMA".
    // Verify the round-trip: host -> net -> host preserves the bit.
    uint32_t flags = 0x1;  // HELLO_ACK_URMA_OK
    uint32_t flags_be = butil::HostToNet32(flags);
    uint32_t flags_back = butil::NetToHost32(flags_be);
    EXPECT_EQ(flags, flags_back);
    EXPECT_NE(0u, flags_back & 0x1);
}

// ---------------------------------------------------------------------------
// ParsedHello field layout: covers the flattened segment (seg_* fields).
// ---------------------------------------------------------------------------
TEST(UrmaHandshakeTest, parsed_hello_segment_fields) {
    urma::ParsedHello p{};
    p.buffer_size = 8192;
    p.recv_buffer_cnt = 127;
    p.jfr_id = 42;
    p.tp_type = 1;
    p.seg_va = 0x1000;
    p.seg_len = 0x100000;
    p.seg_token_id = 7;
    EXPECT_EQ(8192u, p.buffer_size);
    EXPECT_EQ(127u, p.recv_buffer_cnt);
    EXPECT_EQ(42u, p.jfr_id);
    EXPECT_EQ(1u, p.tp_type);
    EXPECT_EQ(0x1000u, p.seg_va);
    EXPECT_EQ(0x100000u, p.seg_len);
    EXPECT_EQ(7u, p.seg_token_id);
}

TEST(UrmaHandshakeTest, rejects_invalid_resource_and_window_values) {
    urma::ParsedHello hello;
    hello.buffer_size = 8192;
    hello.recv_buffer_cnt = 127;
    hello.jfr_id = 1;
    hello.socket_id = 123;
    hello.tp_type = URMA_CTP;
    hello.seg_va = 0x1000;
    hello.seg_len = 8192;
    EXPECT_TRUE(urma::ValidHello(hello));

    hello.recv_buffer_cnt = 0;
    EXPECT_FALSE(urma::ValidHello(hello));
    hello.recv_buffer_cnt = 127;
    hello.seg_len = 0;
    EXPECT_FALSE(urma::ValidHello(hello));
    hello.seg_len = 8192;
    hello.jfr_id = 0;
    EXPECT_FALSE(urma::ValidHello(hello));

    hello.jfr_id = 1;
    hello.socket_id = 123;
    hello.seg_va = std::numeric_limits<uint64_t>::max() - 7;
    hello.seg_len = 8;
    EXPECT_FALSE(urma::ValidHello(hello));
}

TEST(UrmaHelperTest, selects_priority_matching_transport_path_type) {
    urma_device_attr_t attr{};
    attr.dev_cap.priority_info[3].tp_type.bs.rtp = 1;
    attr.dev_cap.priority_info[6].tp_type.bs.ctp = 1;

    EXPECT_EQ(3, urma::FindUrmaPriorityForTpType(attr, URMA_RTP));
    EXPECT_EQ(6, urma::FindUrmaPriorityForTpType(attr, URMA_CTP));
    EXPECT_EQ(-1, urma::FindUrmaPriorityForTpType(attr, URMA_UTP));
}

// ---------------------------------------------------------------------------
// SupportedByUrma: only baidu_std.
// ---------------------------------------------------------------------------
TEST(UrmaHandshakeTest, supported_by_urma_protocol_allowlist) {
    EXPECT_TRUE(urma::SupportedByUrma("baidu_std"));
    EXPECT_FALSE(urma::SupportedByUrma("http"));
    EXPECT_FALSE(urma::SupportedByUrma("hulu_pbrpc"));
    EXPECT_FALSE(urma::SupportedByUrma("nshead"));
}

#else  // BRPC_WITH_URMA

// When URMA is not compiled in, the test file is a no-op so the build stays
// clean. The brpc_urma_unittest target still links (against brpc-shared which
// provides the empty stubs).

#endif  // BRPC_WITH_URMA

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    gflags::ParseCommandLineFlags(&argc, &argv, true);
#if BRPC_WITH_URMA
    urma::g_skip_urma_init = true;
    urma::g_urma_available.store(true, butil::memory_order_relaxed);
#endif
    return RUN_ALL_TESTS();
}
