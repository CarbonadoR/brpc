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
#include <algorithm>
#include <atomic>
#include <set>
#include <thread>
#include <sys/socket.h>
#include <unistd.h>
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
#include "bthread/butex.h"
#include "brpc/server.h"
#include "brpc/input_messenger.h"
#include "brpc/input_message_base.h"
#include "bthread/bthread.h"
#include "bthread/unstable.h"
#include "brpc/controller.h"
#include "echo.pb.h"
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

// ---------------------------------------------------------------------------
// URMA mock smoke test: drives urma_init / device enumeration / context /
// jetty / post / poll. These tests rely on mock semantics and are skipped
// when the test binary links a real liburma provider.
// ---------------------------------------------------------------------------
class UrmaMockTest : public ::testing::Test {
protected:
    void SetUp() override {
        urma::g_skip_urma_init = false;

        urma_init_attr_t init_attr{};
        const urma_status_t status = urma_init(&init_attr);
        ASSERT_TRUE(status == URMA_SUCCESS || status == URMA_EEXIST);
        _owns_urma_init = status == URMA_SUCCESS;

        int num_devices = 0;
        urma_device_t** devices = urma_get_device_list(&num_devices);
        ASSERT_NE(nullptr, devices);
        ASSERT_GT(num_devices, 0);
        const bool using_mock =
            strcmp(devices[0]->name, "mock_urma_device") == 0;
        urma_free_device_list(devices);
        if (!using_mock) {
            if (_owns_urma_init) {
                EXPECT_EQ(URMA_SUCCESS, urma_uninit());
                _owns_urma_init = false;
            }
            GTEST_SKIP() << "UrmaMockTest requires the URMA link-time mock";
        }
    }

    void TearDown() override {
        if (_owns_urma_init) {
            EXPECT_EQ(URMA_SUCCESS, urma_uninit());
        }
        urma::g_skip_urma_init = true;
        urma::g_urma_available.store(true, butil::memory_order_relaxed);
    }

private:
    bool _owns_urma_init{false};
};

TEST_F(UrmaMockTest, init_and_enumerate_device) {
    urma_init_attr_t init_attr{};
    EXPECT_EQ(URMA_EEXIST, urma_init(&init_attr));

    int num_devices = 0;
    urma_device_t** devices = urma_get_device_list(&num_devices);
    ASSERT_NE(nullptr, devices);
    ASSERT_GE(num_devices, 1);
    EXPECT_STREQ("mock_urma_device", devices[0]->name);
    urma_free_device_list(devices);
}

TEST_F(UrmaMockTest, create_context_and_query_device) {
    int num_devices = 0;
    urma_device_t** devices = urma_get_device_list(&num_devices);
    ASSERT_NE(nullptr, devices);
    ASSERT_GE(num_devices, 1);

    uint32_t eid_cnt = 0;
    urma_eid_info_t* eids = urma_get_eid_list(devices[0], &eid_cnt);
    ASSERT_NE(nullptr, eids);
    ASSERT_GE(eid_cnt, 1u);
    urma_free_eid_list(eids);

    urma_context_t* ctx = urma_create_context(devices[0], 0);
    ASSERT_NE(nullptr, ctx);

    urma_device_attr_t attr{};
    ASSERT_EQ(URMA_SUCCESS, urma_query_device(devices[0], &attr));
    EXPECT_GE(attr.dev_cap.max_jfc, 1u);
    EXPECT_GE(attr.dev_cap.max_jetty, 1u);

    EXPECT_EQ(URMA_SUCCESS, urma_delete_context(ctx));
    urma_free_device_list(devices);
}

TEST_F(UrmaMockTest, rejects_send_without_target_jetty) {
    // A SEND without a target jetty is invalid. Keeping the mock strict here
    // prevents tests from relying on input that a real provider cannot post.
    int num_devices = 0;
    urma_device_t** devices = urma_get_device_list(&num_devices);
    ASSERT_NE(nullptr, devices);
    ASSERT_GE(num_devices, 1);
    uint32_t eid_cnt = 0;
    urma_eid_info_t* eids = urma_get_eid_list(devices[0], &eid_cnt);
    urma_free_eid_list(eids);
    urma_context_t* ctx = urma_create_context(devices[0], 0);
    ASSERT_NE(nullptr, ctx);

    urma_jfce_t* jfce = urma_create_jfce(ctx);
    ASSERT_NE(nullptr, jfce);
    urma_jfc_cfg_t jfc_cfg{};
    jfc_cfg.depth = 16;
    jfc_cfg.jfce = jfce;
    urma_jfc_t* jfc = urma_create_jfc(ctx, &jfc_cfg);
    ASSERT_NE(nullptr, jfc);

    urma_jfr_cfg_t jfr_cfg{};
    jfr_cfg.depth = 16;
    jfr_cfg.trans_mode = URMA_TM_RM;
    jfr_cfg.max_sge = 1;
    jfr_cfg.min_rnr_timer = URMA_TYPICAL_MIN_RNR_TIMER;
    jfr_cfg.jfc = jfc;
    urma_jfr_t* jfr = urma_create_jfr(ctx, &jfr_cfg);
    ASSERT_NE(nullptr, jfr);

    urma_jetty_cfg_t jetty_cfg{};
    jetty_cfg.flag.bs.share_jfr = 1;
    jetty_cfg.jfs_cfg.depth = 16;
    jetty_cfg.jfs_cfg.trans_mode = URMA_TM_RM;
    jetty_cfg.jfs_cfg.priority = URMA_MAX_PRIORITY;
    jetty_cfg.jfs_cfg.max_sge = 1;
    jetty_cfg.jfs_cfg.rnr_retry = URMA_TYPICAL_RNR_RETRY;
    jetty_cfg.jfs_cfg.err_timeout = URMA_TYPICAL_ERR_TIMEOUT;
    jetty_cfg.jfs_cfg.jfc = jfc;
    jetty_cfg.shared.jfr = jfr;
    jetty_cfg.shared.jfc = jfc;
    urma_jetty_t* jetty = urma_create_jetty(ctx, &jetty_cfg);
    ASSERT_NE(nullptr, jetty);

    urma_jfs_wr_t wr{};
    memset(&wr, 0, sizeof(wr));
    wr.opcode = URMA_OPC_SEND;
    wr.flag.bs.complete_enable = 1;
    wr.user_ctx = 0xABCD;
    wr.next = nullptr;
    urma_jfs_wr_t* bad = nullptr;
    EXPECT_EQ(URMA_EINVAL, urma_post_jetty_send_wr(jetty, &wr, &bad));
    EXPECT_EQ(&wr, bad);

    urma_cr_t crs[4];
    EXPECT_EQ(0, urma_poll_jfc(jfc, 4, crs));

    urma_delete_jetty(jetty);
    urma_delete_jfr(jfr);
    urma_delete_jfc(jfc);
    urma_delete_jfce(jfce);
    urma_delete_context(ctx);
    urma_free_device_list(devices);
}

TEST_F(UrmaMockTest,
       paired_send_is_bidirectional_and_separates_immediate_credit) {
    int num_devices = 0;
    urma_device_t** devices = urma_get_device_list(&num_devices);
    ASSERT_NE(nullptr, devices);
    ASSERT_GT(num_devices, 0);
    urma_context_t* ctx = urma_create_context(devices[0], 0);
    ASSERT_NE(nullptr, ctx);

    urma_jfce_t* sender_jfce = urma_create_jfce(ctx);
    urma_jfce_t* receiver_jfce = urma_create_jfce(ctx);
    ASSERT_NE(nullptr, sender_jfce);
    ASSERT_NE(nullptr, receiver_jfce);
    urma_jfc_cfg_t sender_jfc_cfg{};
    sender_jfc_cfg.depth = 8;
    sender_jfc_cfg.jfce = sender_jfce;
    urma_jfc_t* sender_jfc = urma_create_jfc(ctx, &sender_jfc_cfg);
    ASSERT_NE(nullptr, sender_jfc);
    urma_jfc_cfg_t receiver_jfc_cfg{};
    receiver_jfc_cfg.depth = 8;
    receiver_jfc_cfg.jfce = receiver_jfce;
    urma_jfc_t* receiver_jfc = urma_create_jfc(ctx, &receiver_jfc_cfg);
    ASSERT_NE(nullptr, receiver_jfc);

    urma_jfr_cfg_t sender_jfr_cfg{};
    sender_jfr_cfg.depth = 4;
    sender_jfr_cfg.trans_mode = URMA_TM_RM;
    sender_jfr_cfg.max_sge = 1;
    sender_jfr_cfg.jfc = sender_jfc;
    urma_jfr_t* sender_jfr = urma_create_jfr(ctx, &sender_jfr_cfg);
    ASSERT_NE(nullptr, sender_jfr);
    urma_jfr_cfg_t receiver_jfr_cfg = sender_jfr_cfg;
    receiver_jfr_cfg.jfc = receiver_jfc;
    urma_jfr_t* receiver_jfr = urma_create_jfr(ctx, &receiver_jfr_cfg);
    ASSERT_NE(nullptr, receiver_jfr);

    auto create_jetty = [&](urma_jfc_t* jfc, urma_jfr_t* jfr) {
        urma_jetty_cfg_t cfg{};
        cfg.flag.bs.share_jfr = 1;
        cfg.jfs_cfg.depth = 4;
        cfg.jfs_cfg.trans_mode = URMA_TM_RM;
        cfg.jfs_cfg.max_sge = 1;
        cfg.jfs_cfg.jfc = jfc;
        cfg.shared.jfr = jfr;
        cfg.shared.jfc = jfc;
        return urma_create_jetty(ctx, &cfg);
    };
    urma_jetty_t* sender = create_jetty(sender_jfc, sender_jfr);
    urma_jetty_t* receiver = create_jetty(receiver_jfc, receiver_jfr);
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);

    urma_rjetty_t remote{};
    remote.jetty_id = receiver->jetty_id;
    remote.trans_mode = URMA_TM_RM;
    remote.type = URMA_JETTY;
    remote.tp_type = URMA_CTP;
    urma_token_t token{};
    urma_target_jetty_t* target =
        urma_import_jetty(ctx, &remote, &token);
    ASSERT_NE(nullptr, target);

    char recv_buf[64]{};
    urma_sge_t recv_sge{
        reinterpret_cast<uint64_t>(recv_buf), sizeof(recv_buf), nullptr,
        nullptr};
    urma_sg_t recv_sg{&recv_sge, 1};
    urma_jfr_wr_t recv_wr{recv_sg, 99, nullptr};
    char credit_recv_buf[1]{};
    urma_sge_t credit_recv_sge{
        reinterpret_cast<uint64_t>(credit_recv_buf),
        sizeof(credit_recv_buf), nullptr, nullptr};
    urma_sg_t credit_recv_sg{&credit_recv_sge, 1};
    urma_jfr_wr_t credit_recv_wr{credit_recv_sg, 100, nullptr};
    recv_wr.next = &credit_recv_wr;
    urma_jfr_wr_t* bad_recv = nullptr;
    ASSERT_EQ(URMA_SUCCESS,
              urma_post_jfr_wr(receiver_jfr, &recv_wr, &bad_recv));

    const char payload[] = "urma-payload";
    urma_sge_t send_sge{
        reinterpret_cast<uint64_t>(payload), sizeof(payload), nullptr,
        nullptr};
    urma_sg_t send_sg{&send_sge, 1};
    urma_jfs_wr_t send_wr{};
    send_wr.opcode = URMA_OPC_SEND;
    send_wr.flag.bs.complete_enable = 1;
    send_wr.tjetty = target;
    send_wr.user_ctx = 7;
    send_wr.send.src = send_sg;
    urma_jfs_wr_t credit_wr{};
    credit_wr.opcode = URMA_OPC_SEND_IMM;
    credit_wr.flag.bs.complete_enable = 1;
    credit_wr.tjetty = target;
    credit_wr.user_ctx = 8;
    credit_wr.send.imm_data = 13;
    send_wr.next = &credit_wr;
    urma_jfs_wr_t* bad_send = nullptr;
    ASSERT_EQ(URMA_SUCCESS,
              urma_post_jetty_send_wr(sender, &send_wr, &bad_send));

    urma_cr_t sender_cr[2]{};
    ASSERT_EQ(2, urma_poll_jfc(sender_jfc, 2, sender_cr));
    EXPECT_EQ(0, sender_cr[0].flag.bs.s_r);
    EXPECT_EQ(7u, sender_cr[0].user_ctx);
    EXPECT_EQ(0, sender_cr[1].flag.bs.s_r);
    EXPECT_EQ(8u, sender_cr[1].user_ctx);

    urma_cr_t receiver_cr[2]{};
    ASSERT_EQ(2, urma_poll_jfc(receiver_jfc, 2, receiver_cr));
    EXPECT_EQ(1, receiver_cr[0].flag.bs.s_r);
    EXPECT_EQ(URMA_CR_OPC_SEND, receiver_cr[0].opcode);
    EXPECT_EQ(0u, receiver_cr[0].imm_data);
    EXPECT_EQ(sizeof(payload), receiver_cr[0].completion_len);
    EXPECT_EQ(0, memcmp(payload, recv_buf, sizeof(payload)));
    EXPECT_EQ(1, receiver_cr[1].flag.bs.s_r);
    EXPECT_EQ(URMA_CR_OPC_SEND_WITH_IMM, receiver_cr[1].opcode);
    EXPECT_EQ(13u, receiver_cr[1].imm_data);
    EXPECT_EQ(0u, receiver_cr[1].completion_len);

    // Exercise the response direction as well. Production posts all receive
    // WRs through the shared JFR.
    urma_rjetty_t sender_remote = remote;
    sender_remote.jetty_id = sender->jetty_id;
    urma_target_jetty_t* sender_target =
        urma_import_jetty(ctx, &sender_remote, &token);
    ASSERT_NE(nullptr, sender_target);
    char response_buf[64]{};
    urma_sge_t response_recv_sge{
        reinterpret_cast<uint64_t>(response_buf), sizeof(response_buf),
        nullptr, nullptr};
    urma_sg_t response_recv_sg{&response_recv_sge, 1};
    urma_jfr_wr_t response_recv_wr{response_recv_sg, 101, nullptr};
    ASSERT_EQ(URMA_SUCCESS,
              urma_post_jfr_wr(sender_jfr, &response_recv_wr, &bad_recv));

    const char response[] = "urma-response";
    urma_sge_t response_send_sge{
        reinterpret_cast<uint64_t>(response), sizeof(response), nullptr,
        nullptr};
    urma_sg_t response_send_sg{&response_send_sge, 1};
    urma_jfs_wr_t response_send_wr{};
    response_send_wr.opcode = URMA_OPC_SEND;
    response_send_wr.flag.bs.complete_enable = 1;
    response_send_wr.tjetty = sender_target;
    response_send_wr.user_ctx = 9;
    response_send_wr.send.src = response_send_sg;
    ASSERT_EQ(URMA_SUCCESS,
              urma_post_jetty_send_wr(receiver, &response_send_wr, &bad_send));

    urma_cr_t response_send_cr{};
    ASSERT_EQ(1, urma_poll_jfc(receiver_jfc, 1, &response_send_cr));
    EXPECT_EQ(0, response_send_cr.flag.bs.s_r);
    EXPECT_EQ(9u, response_send_cr.user_ctx);

    urma_cr_t response_recv_cr{};
    ASSERT_EQ(1, urma_poll_jfc(sender_jfc, 1, &response_recv_cr));
    EXPECT_EQ(1, response_recv_cr.flag.bs.s_r);
    EXPECT_EQ(URMA_CR_OPC_SEND, response_recv_cr.opcode);
    EXPECT_EQ(sizeof(response), response_recv_cr.completion_len);
    EXPECT_EQ(0, memcmp(response, response_buf, sizeof(response)));

    urma_unimport_jetty(sender_target);
    urma_unimport_jetty(target);
    urma_delete_jetty(receiver);
    urma_delete_jetty(sender);
    urma_delete_jfr(receiver_jfr);
    urma_delete_jfr(sender_jfr);
    urma_delete_jfc(receiver_jfc);
    urma_delete_jfc(sender_jfc);
    urma_delete_jfce(receiver_jfce);
    urma_delete_jfce(sender_jfce);
    urma_delete_context(ctx);
    urma_free_device_list(devices);
}

// End-to-end software-provider tests. Each test uses a small pool without an
// event adapter so completions can be delivered in a controlled order.
class UrmaPooledTest : public ::testing::Test {
protected:
    // These pools have no event adapter: tests drive all posts and CQ polling.
    class RecordingPool : public urma::UrmaResourcePool {
    public:
        int PostSend(urma::UrmaJfsSlot* slot, urma_jfs_wr_t* wr,
                     urma::TxCompletionCtx* ctx) override {
            const int rc = UrmaResourcePool::PostSend(slot, wr, ctx);
            if (rc == 0) {
                sends.emplace_back(wr->send.src.sge,
                                   wr->send.src.sge + wr->send.src.num_sge);
            }
            return rc;
        }
        int RepostRecv(urma::UrmaJfrSlot& slot, urma::RxRecvCtx* ctx) override {
            if (fail_next_repost) {
                fail_next_repost = false;
                errno = ENOMEM;
                return -1;
            }
            return UrmaResourcePool::RepostRecv(slot, ctx);
        }
        bool fail_next_repost{false};
        std::vector<std::vector<urma_sge_t>> sends;
    };
    void AppendBlock(butil::IOBuf* data, size_t len, char value) {
        // An explicit block size bypasses TLS so adjacent fragments cannot merge.
        butil::IOBufAsZeroCopyOutputStream stream(data, 8192);
        void* buffer = nullptr;
        int size = 0;
        ASSERT_TRUE(stream.Next(&buffer, &size));
        ASSERT_GE(static_cast<size_t>(size), len);
        memset(buffer, value, len);
        stream.BackUp(size - len);
    }
    static void SetUpTestSuite() {
        urma::g_skip_urma_init = false;
        urma::FLAGS_urma_io_jfs_cnt = 2;
        urma::FLAGS_urma_io_jfr_cnt = 1;
        urma::FLAGS_urma_io_sq_depth = 16;
        urma::FLAGS_urma_io_rx_depth = 8;
        urma::FLAGS_urma_header_buf_count = 512;
        GFLAGS_NAMESPACE::SetCommandLineOption("urma_buffer_count", "2048");
        GFLAGS_NAMESPACE::SetCommandLineOption("urma_use_polling", "false");
        urma::GlobalUrmaInitializeOrDie();
    }
    void SetUp() override {
        if (!urma::GetUrmaContext() ||
            strcmp(urma::GetUrmaContext()->dev->name, "mock_urma_device")) {
            GTEST_SKIP() << "Requires the URMA software provider";
        }
        _original = urma::g_urma_pool;
        _pool.reset(new RecordingPool);
        ASSERT_EQ(0, _pool->Initialize(urma::GetUrmaContext(), {}));
        urma::g_urma_pool = _pool.get();
    }
    void TearDown() override {
        for (auto& socket : _sockets) { socket->SetFailed(); }
        _sockets.clear();
        if (_pool) {
            EXPECT_TRUE(_pool->Shutdown());
            _pool.reset();
            urma::g_urma_pool = _original;
        }
    }
    urma::UrmaEndpoint* Endpoint() {
        SocketOptions options;
        options.socket_mode = SOCKET_MODE_URMA;
        SocketId sid;
        if (Socket::Create(options, &sid) != 0) { return nullptr; }
        SocketUniquePtr socket;
        if (Socket::Address(sid, &socket) != 0) { return nullptr; }
        auto* ep = static_cast<UrmaTransport*>(socket->_transport.get())->GetUrmaEp();
        if (ep->AllocateResources() != 0) { return nullptr; }
        ep->_state = urma::UrmaEndpoint::S_ACK_WAIT;
        _sockets.push_back(std::move(socket));
        return ep;
    }
    void Connect(urma::UrmaEndpoint* from, urma::UrmaEndpoint* to) {
        urma::ParsedHello hello;
        to->MakeLocalParsedHello(&hello);
        ASSERT_TRUE(urma::ValidHello(hello));
        from->ApplyRemoteHello(hello);
        ASSERT_EQ(0, from->ImportPeer(hello));
    }
    void DrainTx() {
        for (auto* slot : _pool->io_jfs) {
            urma_cr_t cr;
            while (urma_poll_jfc(slot->jfc, 1, &cr) == 1) {
                urma::UrmaEndpoint::HandleTxCompletion(slot, cr);
            }
        }
    }
    void DrainRx(bool reverse = false) {
        for (auto* slot : _pool->io_jfr) {
            std::vector<urma_cr_t> completions;
            urma_cr_t cr;
            while (urma_poll_jfc(slot->jfc, 1, &cr) == 1) { completions.push_back(cr); }
            if (reverse) { std::reverse(completions.begin(), completions.end()); }
            for (const auto& completion : completions) {
                urma::UrmaEndpoint::HandleRxCompletion(slot, completion);
            }
        }
    }
    void CheckBlockingCallback();
    std::unique_ptr<RecordingPool> _pool;
    urma::UrmaResourcePool* _original{nullptr};
    std::vector<SocketUniquePtr> _sockets;
};

TEST_F(UrmaPooledTest, registered_header_pool_exhaustion_and_concurrency) {
    auto memory = urma::CreateUrmaMemoryPool(urma::GetUrmaContext(), 64, 128);
    ASSERT_TRUE(memory);
    std::set<void*> addresses;
    for (size_t i = 0; i < 128; ++i) {
        void* p = memory->Allocate(64);
        ASSERT_NE(nullptr, p);
        EXPECT_EQ(0u, reinterpret_cast<uintptr_t>(p) % 64);
        EXPECT_TRUE(addresses.insert(p).second);
        EXPECT_NE(nullptr, memory->SegmentFor(static_cast<char*>(p) + 63));
    }
    EXPECT_EQ(nullptr, memory->Allocate(64));
    EXPECT_EQ(128u, memory->outstanding());
    for (void* p : addresses) { memory->Deallocate(p); }
    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i) {
        threads.emplace_back([&] {
            for (int n = 0; n < 2000; ++n) {
                void* p = memory->Allocate(64);
                ASSERT_NE(nullptr, p);
                std::memset(p, 42, 64);
                memory->Deallocate(p);
            }
        });
    }
    for (auto& thread : threads) { thread.join(); }
    EXPECT_EQ(0u, memory->outstanding());
    EXPECT_EQ(nullptr, memory->Allocate(65));
    EXPECT_TRUE(memory->Unregister());
    EXPECT_EQ(nullptr, memory->Allocate(64));
}

TEST_F(UrmaPooledTest, independent_queues_and_import_cache) {
    auto* a = Endpoint();
    auto* b = Endpoint();
    auto* c = Endpoint();
    ASSERT_NE(nullptr, a);
    ASSERT_NE(nullptr, b);
    ASSERT_NE(nullptr, c);
    Connect(a, b);
    Connect(c, b);
    EXPECT_EQ(a->_conn_res->remote_jfr, c->_conn_res->remote_jfr);
    EXPECT_EQ(1u, _pool->imports->size());
    EXPECT_NE(_pool->io_jfs[0]->jfc, _pool->io_jfr[0]->jfc);
    EXPECT_EQ(2u, _pool->io_jfr[0]->jfr->jfr_cfg.max_sge);
    for (auto* slot : _pool->io_jfs) {
        EXPECT_EQ(BRPC_URMA_SINGLE_SGE ? 1 : urma::GetUrmaMaxSge(),
                  slot->jfs->jfs_cfg.max_sge);
    }
    EXPECT_EQ(8u, _pool->io_jfr[0]->pending.size());
    a->Reset();
    EXPECT_EQ(1u, _pool->imports->size());
    c->Reset();
    EXPECT_EQ(0u, _pool->imports->size());
    EXPECT_EQ(8u, _pool->io_jfr[0]->pending.size());
}

TEST_F(UrmaPooledTest, pooled_send_reorder_and_wrap_preserve_stream) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);
    Connect(sender, receiver);
    sender->_next_send_seq = 65534;
    receiver->_deliver_base = 65534;
    butil::IOBuf data;
    const std::string expected(20000, 'x');
    data.append(expected);
    const void* first_body = data.fetch1();
    butil::IOBuf* input[] = {&data};
    ASSERT_EQ(static_cast<ssize_t>(expected.size()), sender->CutFromIOBufList(input, 1));
    EXPECT_TRUE(data.empty());
    EXPECT_GT(_pool->io_jfs[0]->pending.size(), 0u);
    EXPECT_GT(_pool->io_jfs[1]->pending.size(), 0u);
    for (auto* slot : _pool->io_jfs) {
        for (const auto* ctx : slot->pending) {
            EXPECT_FALSE(ctx->send_data.empty());
            EXPECT_EQ(urma::MSG_SMALL, ctx->header_buf->type);
            EXPECT_EQ(receiver->_self_socket_id,
                      butil::NetToHost64(ctx->header_buf->dst_socket_id));
            if (ctx->seq == 65534) {
#if BRPC_URMA_SINGLE_SGE
                EXPECT_NE(first_body, ctx->send_data.fetch1());
#else
                EXPECT_EQ(first_body, ctx->send_data.fetch1());
#endif
            }
        }
    }
    DrainRx(true);
    EXPECT_EQ(expected, receiver->_input_processor.read_buf().to_string());
    EXPECT_TRUE(receiver->_reorder_queue.empty());
    EXPECT_LT(receiver->_deliver_base, 10u);
    DrainTx();
    for (auto* slot : _pool->io_jfs) {
        EXPECT_EQ(0u, slot->sq_inflight.load());
        EXPECT_TRUE(slot->pending.empty());
    }
}

TEST_F(UrmaPooledTest, ctp_limits_total_wire_length_in_both_sge_modes) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);
    Connect(sender, receiver);
    // Use a fresh contiguous block, so the split must come from the CTP
    // wire limit rather than an existing IOBuf block boundary.
    butil::IOBuf data;
    {
        butil::IOBufAsZeroCopyOutputStream stream(&data, 8192);
        void* buffer = nullptr;
        int size = 0;
        ASSERT_TRUE(stream.Next(&buffer, &size));
        ASSERT_GE(size, 4033);
        memset(buffer, 'x', 4033);
        stream.BackUp(size - 4033);
    }
    butil::IOBuf* input[] = {&data};
    ASSERT_EQ(4033, sender->CutFromIOBufList(input, 1));
    EXPECT_EQ(2u, sender->_next_send_seq);
    auto* slot = _pool->io_jfr[0];
    std::vector<uint32_t> lengths;
    urma_cr_t cr;
    while (urma_poll_jfc(slot->jfc, 1, &cr) == 1) {
        lengths.push_back(cr.completion_len);
        urma::UrmaEndpoint::HandleRxCompletion(slot, cr);
    }
    ASSERT_EQ(2u, lengths.size());
    EXPECT_EQ(4096u, lengths[0]);
    EXPECT_EQ(65u, lengths[1]);
    EXPECT_EQ(std::string(4033, 'x'), receiver->_input_processor.read_buf().to_string());
    DrainTx();
}

TEST_F(UrmaPooledTest, post_failure_preserves_caller_data_and_sequence) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);
    Connect(sender, receiver);
    for (int i = 0; i < 7; ++i) {
        butil::IOBuf data;
        data.append("a");
        butil::IOBuf* input[] = {&data};
        ASSERT_EQ(1, sender->CutFromIOBufList(input, 1));
    }
    butil::IOBuf first, remaining, last, empty;
    AppendBlock(&first, 1000, 'b');
    AppendBlock(&remaining, 5000, 'c');
    AppendBlock(&last, 500, 'd');
    butil::IOBuf* input[] = {&first, &empty, &remaining, &last};
    // Only one receive remains. Commit a full message spanning two inputs,
    // then fail the next post without consuming any of its gathered fragments.
    EXPECT_EQ(4032, sender->CutFromIOBufList(input, 4));
    EXPECT_TRUE(first.empty());
    EXPECT_EQ(std::string(1968, 'c'), remaining.to_string());
    EXPECT_EQ(std::string(500, 'd'), last.to_string());
    EXPECT_EQ(-1, sender->CutFromIOBufList(input, 4));
    EXPECT_EQ(EAGAIN, errno);
    EXPECT_EQ(std::string(1968, 'c'), remaining.to_string());
    EXPECT_EQ(std::string(500, 'd'), last.to_string());
    EXPECT_EQ(8u, sender->_next_send_seq);
    DrainRx();
    DrainTx();
    EXPECT_EQ(2468, sender->CutFromIOBufList(input, 4));
    DrainRx();
    DrainTx();
    EXPECT_EQ(std::string(7, 'a') + std::string(1000, 'b') +
                  std::string(5000, 'c') + std::string(500, 'd'),
              receiver->_input_processor.read_buf().to_string());
}

TEST_F(UrmaPooledTest, gather_across_iobufs_until_ctp_body_limit) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);
    Connect(sender, receiver);
    butil::IOBuf first, second, empty;
    AppendBlock(&first, 1000, 'a');
    AppendBlock(&first, 2000, 'b');
    AppendBlock(&second, 1033, 'c');
#if !BRPC_URMA_SINGLE_SGE
    const void* addresses[] = {first.backing_block(0).data(),
                              first.backing_block(1).data(), second.fetch1()};
#endif
    const std::string expected = first.to_string() + second.to_string();
    butil::IOBuf* input[] = {&empty, &first, &empty, &second, &empty};
    ASSERT_EQ(4033, sender->CutFromIOBufList(input, 5));
    EXPECT_TRUE(first.empty());
    EXPECT_TRUE(second.empty());
    EXPECT_EQ(2u, sender->_next_send_seq);
    ASSERT_EQ(2u, _pool->sends.size());
#if BRPC_URMA_SINGLE_SGE
    EXPECT_EQ(1u, _pool->sends[0].size());
    EXPECT_EQ(4096u, _pool->sends[0][0].len);
    EXPECT_EQ(65u, _pool->sends[1][0].len);
#else
    ASSERT_EQ(4u, _pool->sends[0].size());
    EXPECT_EQ(64u, _pool->sends[0][0].len);
    EXPECT_EQ(1000u, _pool->sends[0][1].len);
    EXPECT_EQ(2000u, _pool->sends[0][2].len);
    EXPECT_EQ(1032u, _pool->sends[0][3].len);
    for (size_t i = 0; i < 3; ++i) {
        EXPECT_EQ(reinterpret_cast<uint64_t>(addresses[i]), _pool->sends[0][i + 1].addr);
    }
    ASSERT_EQ(2u, _pool->sends[1].size());
    EXPECT_EQ(1u, _pool->sends[1][1].len);
    EXPECT_EQ(reinterpret_cast<uint64_t>(addresses[2]) + 1032,
              _pool->sends[1][1].addr);
#endif
    DrainRx(true);
    EXPECT_EQ(expected, receiver->_input_processor.read_buf().to_string());
    DrainTx();
}

TEST_F(UrmaPooledTest, gather_stops_at_sge_limit_only_in_zero_copy_mode) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);
    Connect(sender, receiver);
    const size_t max_sge = urma::GetUrmaMaxSge();
    const size_t fragments = max_sge + 2;
    ASSERT_LT((max_sge - 1) * 512, 4032u);
    butil::IOBuf first, second, empty;
    std::string expected;
    for (size_t i = 0; i < fragments; ++i) {
        AppendBlock(i < max_sge / 2 ? &first : &second, 512, 'a' + i);
        expected.append(512, 'a' + i);
    }
    butil::IOBuf* input[] = {&first, &empty, &second};
    ASSERT_EQ(static_cast<ssize_t>(expected.size()), sender->CutFromIOBufList(input, 3));
#if BRPC_URMA_SINGLE_SGE
    ASSERT_EQ((expected.size() + 4031) / 4032, _pool->sends.size());
    size_t remaining = expected.size();
    for (const auto& send : _pool->sends) {
        ASSERT_EQ(1u, send.size());
        const size_t body = std::min<size_t>(remaining, 4032);
        EXPECT_EQ(64 + body, send[0].len);
        remaining -= body;
    }
#else
    ASSERT_EQ((fragments + max_sge - 2) / (max_sge - 1), _pool->sends.size());
    ASSERT_EQ(max_sge, _pool->sends[0].size());
    for (const auto& send : _pool->sends) {
        EXPECT_LE(send.size(), max_sge);
        EXPECT_EQ(64u, send[0].len);
        for (size_t i = 1; i < send.size(); ++i) { EXPECT_EQ(512u, send[i].len); }
    }
#endif
    DrainRx(true);
    EXPECT_EQ(expected, receiver->_input_processor.read_buf().to_string());
    DrainTx();
}

TEST_F(UrmaPooledTest, gather_respects_smaller_peer_receive_capacity) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);
    Connect(sender, receiver);
    sender->_remote_recv_block_size = 128;
    butil::IOBuf first, second;
    AppendBlock(&first, 64, 'a');
    AppendBlock(&second, 110, 'b');
    butil::IOBuf* input[] = {&first, &second};
    ASSERT_EQ(174, sender->CutFromIOBufList(input, 2));
    ASSERT_EQ(2u, _pool->sends.size());
    for (size_t i = 0; i < 2; ++i) {
        size_t wire_len = 0;
        for (const auto& sge : _pool->sends[i]) { wire_len += sge.len; }
        EXPECT_EQ(i == 0 ? 192u : 110u, wire_len);
    }
    DrainRx();
    EXPECT_EQ(std::string(64, 'a') + std::string(110, 'b'),
              receiver->_input_processor.read_buf().to_string());
    DrainTx();
}

TEST_F(UrmaPooledTest, gather_retains_each_user_registered_segment) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);
    Connect(sender, receiver);
    auto released = std::make_shared<int>(0);
    butil::IOBuf data;
    std::vector<uint64_t> segments;
    for (int i = 0; i < 3; ++i) {
        void* block = malloc(100);
        ASSERT_NE(nullptr, block);
        memset(block, 'a' + i, 100);
        const uint64_t segment = urma::RegisterMemoryForUrma(block, 100);
        ASSERT_NE(0u, segment);
        segments.push_back(segment);
        ASSERT_EQ(0, data.append_user_data_with_meta(block, 100, [released](void* p) {
            urma::DeregisterMemoryForUrma(p);
            free(p);
            ++*released;
        }, segment));
    }
    butil::IOBuf* input[] = {&data};
    ASSERT_EQ(300, sender->CutFromIOBufList(input, 1));
    EXPECT_TRUE(data.empty());
    ASSERT_EQ(1u, _pool->sends.size());
#if BRPC_URMA_SINGLE_SGE
    EXPECT_EQ(3, *released);
#else
    EXPECT_EQ(0, *released);
    ASSERT_EQ(4u, _pool->sends[0].size());
    for (size_t i = 0; i < segments.size(); ++i) {
        EXPECT_EQ(segments[i], reinterpret_cast<uint64_t>(_pool->sends[0][i + 1].tseg));
    }
#endif
    DrainRx();
    EXPECT_EQ(std::string(100, 'a') + std::string(100, 'b') + std::string(100, 'c'),
              receiver->_input_processor.read_buf().to_string());
    DrainTx();
    EXPECT_EQ(3, *released);
}

TEST_F(UrmaPooledTest, gather_unregistered_fragment_does_not_consume_input) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);
    Connect(sender, receiver);
    butil::IOBuf first, second;
    AppendBlock(&first, 100, 'a');
    void* unregistered = malloc(100);
    ASSERT_NE(nullptr, unregistered);
    memset(unregistered, 'b', 100);
    ASSERT_EQ(0, second.append_user_data(unregistered, 100, free));
    butil::IOBuf* input[] = {&first, &second};
#if BRPC_URMA_SINGLE_SGE
    EXPECT_EQ(200, sender->CutFromIOBufList(input, 2));
    DrainRx();
    DrainTx();
    EXPECT_EQ(std::string(100, 'a') + std::string(100, 'b'),
              receiver->_input_processor.read_buf().to_string());
#else
    EXPECT_EQ(-1, sender->CutFromIOBufList(input, 2));
    EXPECT_EQ(ERDMAMEM, errno);
    EXPECT_EQ(std::string(100, 'a'), first.to_string());
    EXPECT_EQ(std::string(100, 'b'), second.to_string());
    EXPECT_EQ(0u, sender->_next_send_seq);
    EXPECT_TRUE(_pool->sends.empty());
    for (auto* slot : _pool->io_jfs) {
        EXPECT_EQ(0u, slot->sq_inflight.load());
        EXPECT_TRUE(slot->pending.empty());
    }
#endif
}

TEST_F(UrmaPooledTest, disconnect_keeps_dma_and_imports_alive_until_completion) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);
    Connect(sender, receiver);
    std::weak_ptr<urma::UrmaConnResource> resource = sender->_conn_res;
    butil::IOBuf data;
    data.append("in-flight");
    butil::IOBuf* input[] = {&data};
    ASSERT_EQ(9, sender->CutFromIOBufList(input, 1));
    sender->Reset();
    EXPECT_FALSE(resource.expired());
    EXPECT_EQ(1u, _pool->imports->size());
    _sockets[0]->SetFailed();
    _sockets[1]->SetFailed();
    DrainTx();
    EXPECT_TRUE(resource.expired());
    EXPECT_EQ(0u, _pool->imports->size());
    DrainRx();
    EXPECT_EQ(8u, _pool->io_jfr[0]->pending.size());
}

TEST_F(UrmaPooledTest, receive_errors_repost_and_reset_only_connection_state) {
    auto* ep = Endpoint();
    ASSERT_NE(nullptr, ep);
    SocketUniquePtr socket;
    ASSERT_EQ(0, Socket::Address(ep->_self_socket_id, &socket));
    butil::IOBuf second;
    second.append("second");
    ep->ReorderAndDispatch(socket, 1, second);
    EXPECT_EQ(1u, ep->_reorder_queue.size());
    EXPECT_TRUE(ep->_input_processor.read_buf().empty());
    ep->Reset();
    EXPECT_TRUE(ep->_reorder_queue.empty());
    EXPECT_EQ(0u, ep->_deliver_base);
    EXPECT_EQ(0u, ep->_next_send_seq);
    EXPECT_EQ(8u, _pool->io_jfr[0]->pending.size());
}

TEST_F(UrmaPooledTest, duplicate_sequence_fails_connection) {
    auto* ep = Endpoint();
    ASSERT_NE(nullptr, ep);
    SocketUniquePtr socket;
    ASSERT_EQ(0, Socket::Address(ep->_self_socket_id, &socket));
    butil::IOBuf data;
    data.append("a");
    ep->ReorderAndDispatch(socket, 1, data);
    ep->ReorderAndDispatch(socket, 1, data);
    EXPECT_TRUE(socket->Failed());
}

TEST_F(UrmaPooledTest, receive_reposting_preserves_delivered_buffers) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);
    Connect(sender, receiver);
    auto* slot = _pool->io_jfr[0];
    const auto contexts = slot->pending;
    std::string expected;
    // Cycle every receive slot several times while the reader retains all
    // earlier data. Alternate copied and zero-copy receives.
    for (int i = 0; i < 32; ++i) {
        const std::string packet(i % 2 ? 1024 : 16, 'a' + i % 26);
        butil::IOBuf data;
        data.append(packet);
        butil::IOBuf* input[] = {&data};
        ASSERT_EQ(static_cast<ssize_t>(packet.size()),
                  sender->CutFromIOBufList(input, 1));
        DrainTx();
        DrainRx();
        expected += packet;
        EXPECT_EQ(expected, receiver->_input_processor.read_buf().to_string());
        EXPECT_EQ(contexts, slot->pending);
    }
}

TEST_F(UrmaPooledTest, single_sge_header_uses_owned_wire_buffer) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);
    Connect(sender, receiver);
    std::vector<void*> held_headers;
    while (void* header = urma::AllocHeaderBuffer()) {
        held_headers.push_back(header);
    }
    butil::IOBuf data;
    data.append("body");
    butil::IOBuf* input[] = {&data};
    const ssize_t sent = sender->CutFromIOBufList(input, 1);
#if BRPC_URMA_SINGLE_SGE
    EXPECT_EQ(4, sent);
    EXPECT_TRUE(data.empty());
    for (const auto* slot : _pool->io_jfs) {
        for (const auto* ctx : slot->pending) {
            EXPECT_EQ(ctx->send_data.fetch1(),
                      static_cast<const void*>(ctx->header_buf));
            EXPECT_NE(nullptr, urma::GetPoolSegFor(ctx->header_buf));
            EXPECT_EQ(nullptr, urma::GetHeaderPoolSegFor(ctx->header_buf));
        }
    }
#else
    EXPECT_EQ(-1, sent);
    EXPECT_EQ("body", data.to_string());
#endif
    for (void* header : held_headers) { urma::FreeHeaderBuffer(header); }
    DrainTx();
    DrainRx();
#if BRPC_URMA_SINGLE_SGE
    EXPECT_EQ("body", receiver->_input_processor.read_buf().to_string());
#endif
}

TEST_F(UrmaPooledTest, failed_repost_keeps_context_owned_until_shutdown) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);
    Connect(sender, receiver);
    SocketUniquePtr socket;
    ASSERT_EQ(0, Socket::Address(receiver->_self_socket_id, &socket));
    auto* slot = _pool->io_jfr[0];
    const auto contexts = slot->pending;
    butil::IOBuf data;
    data.append(std::string(1024, 'x'));
    butil::IOBuf* input[] = {&data};
    ASSERT_EQ(1024, sender->CutFromIOBufList(input, 1));
    _pool->fail_next_repost = true;
    DrainRx();
    EXPECT_TRUE(socket->Failed());
    EXPECT_TRUE(slot->stopped.load());
    EXPECT_EQ(contexts, slot->pending);
    DrainTx();
    ASSERT_TRUE(_pool->Shutdown());
    EXPECT_TRUE(slot->pending.empty());
}

TEST_F(UrmaPooledTest, ordered_receive_drains_gap_across_sequence_wrap) {
    auto* ep = Endpoint();
    ASSERT_NE(nullptr, ep);
    SocketUniquePtr socket;
    ASSERT_EQ(0, Socket::Address(ep->_self_socket_id, &socket));
    ep->_deliver_base = 65535;
    auto receive = [&](uint16_t seq, const char* text) {
        butil::IOBuf data;
        data.append(text);
        ep->ReorderAndDispatch(socket, seq, std::move(data));
    };
    receive(65535, "a");
    EXPECT_TRUE(ep->_reorder_queue.empty());
    EXPECT_EQ(0u, ep->_deliver_base);
    receive(1, "c");
    EXPECT_EQ("a", ep->_input_processor.read_buf().to_string());
    ASSERT_EQ(1u, ep->_reorder_queue.size());
    receive(0, "b");
    EXPECT_EQ("abc", ep->_input_processor.read_buf().to_string());
    EXPECT_TRUE(ep->_reorder_queue.empty());
    EXPECT_EQ(2u, ep->_deliver_base);
    EXPECT_FALSE(socket->Failed());
    // A duplicate of a directly delivered packet must still fail.
    receive(0, "duplicate");
    EXPECT_TRUE(socket->Failed());
    EXPECT_EQ("abc", ep->_input_processor.read_buf().to_string());
}

TEST_F(UrmaPooledTest, shared_sq_completion_wakes_other_connection) {
    auto* waiting = Endpoint();
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_NE(nullptr, waiting);
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);
    Connect(sender, receiver);
    butil::IOBuf data;
    data.append("complete on a different connection");
    butil::IOBuf* input[] = {&data};
    ASSERT_GT(sender->CutFromIOBufList(input, 1), 0);
    // Reserve the rest of the shared capacity, without any WR owned by waiting.
    while (_pool->AcquireJfs()) {}
    ASSERT_FALSE(_pool->IsWritable());
    auto* socket = _sockets[0].get();
    auto* transport = static_cast<UrmaTransport*>(socket->_transport.get());
    transport->_urma_state = UrmaTransport::URMA_ON;
    std::atomic<bool> entered{false};
    int64_t elapsed = 0;
    std::thread writer([&] {
        const auto deadline = butil::milliseconds_from_now(500);
        const int64_t start = butil::gettimeofday_us();
        entered.store(true);
        transport->WaitEpollOut(socket->_epollout_butex, true, deadline);
        elapsed = butil::gettimeofday_us() - start;
    });
    while (!entered.load()) { std::this_thread::yield(); }
    usleep(20000);
    DrainTx();
    writer.join();
    EXPECT_LT(elapsed, 250000) << "Shared capacity must not wait for timeout";
    for (auto* slot : _pool->io_jfs) { slot->sq_inflight.store(0); }
    DrainRx();
}

TEST_F(UrmaPooledTest, shared_sq_wait_handles_preexisting_space_and_notification) {
    auto* bx = bthread::butex_create_checked<butil::atomic<int>>();
    bx->store(0);
    auto deadline = butil::milliseconds_from_now(100);
    // Completion before registration: never sleep with space already available.
    EXPECT_EQ(0, _pool->WaitWritable(bx, 0, &deadline));
    EXPECT_FALSE(_pool->_has_writers.load());
    while (_pool->AcquireJfs()) {}
    // An earlier Socket notification must not be lost before entering wait.
    bx->fetch_add(1);
    EXPECT_EQ(-1, _pool->WaitWritable(bx, 0, &deadline));
    EXPECT_EQ(EWOULDBLOCK, errno);
    EXPECT_FALSE(_pool->_has_writers.load());
    // Timed-out stack waiters must be removed before their butex is freed.
    deadline = butil::milliseconds_from_now(2);
    EXPECT_EQ(-1, _pool->WaitWritable(bx, 1, &deadline));
    EXPECT_EQ(ETIMEDOUT, errno);
    EXPECT_FALSE(_pool->_has_writers.load());
    bthread::butex_destroy(bx);
    _pool->ReleaseJfs(_pool->io_jfs[0]);
    for (auto* slot : _pool->io_jfs) { slot->sq_inflight.store(0); }
}

TEST_F(UrmaPooledTest, shared_sq_wait_registration_races_credit_release) {
    auto* bx = bthread::butex_create_checked<butil::atomic<int>>();
    bx->store(0);
    std::atomic<int> timeouts{0};
    for (int i = 0; i < 200; ++i) {
        while (_pool->AcquireJfs()) {}
        const int expected = bx->load();
        std::thread release([&] { _pool->ReleaseJfs(_pool->io_jfs[0]); });
        const auto deadline = butil::milliseconds_from_now(100);
        if (_pool->WaitWritable(bx, expected, &deadline) < 0 && errno == ETIMEDOUT) {
            ++timeouts;
        }
        release.join();
        EXPECT_FALSE(_pool->_has_writers.load());
    }
    EXPECT_EQ(0, timeouts.load());
    bthread::butex_destroy(bx);
    for (auto* slot : _pool->io_jfs) { slot->sq_inflight.store(0); }
}

TEST_F(UrmaPooledTest, shared_sq_wait_cancellation_preserves_other_waiters) {
    while (_pool->AcquireJfs()) {}
    butil::atomic<int>* bx[3];
    std::thread writers[3];
    std::atomic<int> timeouts{0};
    for (int i = 0; i < 3; ++i) {
        bx[i] = bthread::butex_create_checked<butil::atomic<int>>();
        bx[i]->store(0);
        writers[i] = std::thread([&, i] {
            const auto deadline = butil::milliseconds_from_now(1000);
            if (_pool->WaitWritable(bx[i], 0, &deadline) < 0 && errno == ETIMEDOUT) {
                ++timeouts;
            }
        });
        // Order registration so the next cancellation removes the middle node.
        const int64_t end = butil::gettimeofday_us() + 500000;
        size_t queued = 0;
        do {
            {
                BAIDU_SCOPED_LOCK(_pool->_writers_mutex);
                queued = 0;
                for (auto* node = _pool->_writers; node; node = node->next) { ++queued; }
            }
            if (queued == static_cast<size_t>(i + 1)) { break; }
            std::this_thread::yield();
        } while (butil::gettimeofday_us() < end);
        EXPECT_EQ(static_cast<size_t>(i + 1), queued);
    }
    // Socket failure/cancellation uses this same butex and must unlink safely.
    bx[1]->fetch_add(1);
    bthread::butex_wake_all(bx[1]);
    writers[1].join();
    bthread::butex_destroy(bx[1]);
    _pool->ReleaseJfs(_pool->io_jfs[0]);
    for (int i : {0, 2}) {
        writers[i].join();
        bthread::butex_destroy(bx[i]);
    }
    EXPECT_EQ(0, timeouts.load());
    EXPECT_FALSE(_pool->_has_writers.load());
    for (auto* slot : _pool->io_jfs) { slot->sq_inflight.store(0); }
}

TEST_F(UrmaPooledTest, shared_sq_shutdown_notifies_registered_waiters) {
    while (_pool->AcquireJfs()) {}
    auto* bx = bthread::butex_create_checked<butil::atomic<int>>();
    bx->store(0);
    std::atomic<int> result{-2};
    std::thread writer([&] {
        const auto deadline = butil::milliseconds_from_now(1000);
        const int rc = _pool->WaitWritable(bx, 0, &deadline);
        result.store(rc == 0 || errno == EWOULDBLOCK ? 0 : errno);
    });
    while (!_pool->_has_writers.load()) { std::this_thread::yield(); }
    EXPECT_TRUE(_pool->Shutdown());
    writer.join();
    EXPECT_EQ(0, result.load());
    EXPECT_FALSE(_pool->_has_writers.load());
    const auto deadline = butil::milliseconds_from_now(1);
    EXPECT_EQ(-1, _pool->WaitWritable(bx, bx->load(), &deadline));
    EXPECT_EQ(EIO, errno);
    bthread::butex_destroy(bx);
}

TEST_F(UrmaPooledTest, shared_sq_shutdown_drains_returning_guards) {
    auto* bx = bthread::butex_create_checked<butil::atomic<int>>();
    bx->store(0);
    std::atomic<bool> entered{false}, release{false}, stopped{false};
    std::thread writer([&] {
        urma::UrmaResourcePool::WriteWaiter waiter(_pool.get(), bx);
        entered.store(true);
        while (!release.load()) { std::this_thread::yield(); }
    });
    while (!entered.load()) { std::this_thread::yield(); }
    std::thread shutdown([&] {
        EXPECT_TRUE(_pool->Shutdown());
        stopped.store(true);
    });
    // Wait until shutdown has closed registration, while the returning guard
    // deliberately remains alive and no longer belongs to the waiter list.
    for (;;) {
        bool closed;
        {
            BAIDU_SCOPED_LOCK(_pool->_writers_mutex);
            closed = _pool->_writers_closed;
        }
        if (closed) { break; }
        std::this_thread::yield();
    }
    EXPECT_FALSE(stopped.load());
    release.store(true);
    writer.join();
    shutdown.join();
    EXPECT_TRUE(stopped.load());
    EXPECT_EQ(0u, _pool->_active_writers.load());
    bthread::butex_destroy(bx);
}

TEST_F(UrmaPooledTest, concurrent_sq_reservations_do_not_exceed_depth) {
    std::atomic<int> acquired{0};
    std::vector<std::thread> threads;
    for (int i = 0; i < 8; ++i) {
        threads.emplace_back([&] {
            while (_pool->AcquireJfs()) { acquired.fetch_add(1); }
        });
    }
    for (auto& thread : threads) { thread.join(); }
    EXPECT_EQ(32, acquired.load());
    EXPECT_FALSE(_pool->IsWritable());
    for (auto* slot : _pool->io_jfs) {
        EXPECT_EQ(slot->sq_depth, slot->sq_inflight.load());
        slot->sq_inflight.store(0);
    }
}

TEST_F(UrmaPooledTest, shutdown_reclaims_unpolled_contexts) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);
    Connect(sender, receiver);
    butil::IOBuf data;
    data.append("shutdown");
    butil::IOBuf* input[] = {&data};
    ASSERT_EQ(8, sender->CutFromIOBufList(input, 1));
    sender->Reset();
    receiver->Reset();
    ASSERT_TRUE(_pool->Shutdown());
    EXPECT_EQ(0u, _pool->imports->size());
    for (auto* slot : _pool->io_jfs) { EXPECT_TRUE(slot->pending.empty()); }
    for (auto* slot : _pool->io_jfr) { EXPECT_TRUE(slot->pending.empty()); }
    EXPECT_TRUE(_pool->Shutdown());
}

TEST_F(UrmaPooledTest, initialization_failure_releases_preposted_buffers) {
    auto free_headers = []() {
        std::vector<void*> headers;
        while (void* p = urma::AllocHeaderBuffer()) { headers.push_back(p); }
        const size_t count = headers.size();
        for (void* p : headers) { urma::FreeHeaderBuffer(p); }
        return count;
    };
    const size_t before = free_headers();
    {
        urma::UrmaResourcePool failed;
        urma::UrmaResourcePool::EventAdapter events;
        events.attach = [](urma::UrmaIoSlot*) { errno = EMFILE; return -1; };
        EXPECT_EQ(-1, failed.Initialize(urma::GetUrmaContext(), events));
        EXPECT_LT(free_headers(), before);
        EXPECT_TRUE(failed.Shutdown());
    }
    EXPECT_EQ(before, free_headers());
}

TEST_F(UrmaPooledTest, legacy_binary_hello_is_rejected_without_waiting_for_extension) {
    int fds[2];
    ASSERT_EQ(0, socketpair(AF_UNIX, SOCK_STREAM, 0, fds));
    auto* ep = Endpoint();
    ASSERT_NE(nullptr, ep);
    // No event consumer: ReadBodyAndNegotiate is driven directly by this test.
    ep->_socket->_transport->_on_edge_trigger = nullptr;
    ASSERT_EQ(0, ep->_socket->ResetFileDescriptor(fds[0]));
    uint16_t prefix[3] = {butil::HostToNet16(86), butil::HostToNet16(2),
                          butil::HostToNet16(1)};
    ASSERT_EQ(sizeof(prefix), write(fds[1], prefix, sizeof(prefix)));
    urma::ParsedHello hello;
    bool negotiated = true;
    EXPECT_EQ(-1, urma::ReadBodyAndNegotiate(ep, &hello, &negotiated));
    EXPECT_EQ(EPROTO, errno);
    EXPECT_FALSE(negotiated);
    close(fds[1]);
}

TEST_F(UrmaPooledTest, failed_and_short_rx_completions_reclaim_and_repost) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);
    Connect(sender, receiver);
    for (int i = 0; i < 2; ++i) {
        butil::IOBuf data;
        data.append("x");
        butil::IOBuf* input[] = {&data};
        ASSERT_EQ(1, sender->CutFromIOBufList(input, 1));
    }
    auto* slot = _pool->io_jfr[0];
    urma_cr_t cr[2];
    ASSERT_EQ(2, urma_poll_jfc(slot->jfc, 2, cr));
    cr[0].status = URMA_CR_RNR_RETRY_CNT_EXC_ERR;
    cr[1].completion_len = 3;
    urma::UrmaEndpoint::HandleRxCompletion(slot, cr[0]);
    urma::UrmaEndpoint::HandleRxCompletion(slot, cr[1]);
    EXPECT_EQ(8u, slot->pending.size());
    EXPECT_TRUE(receiver->_input_processor.read_buf().empty());
    DrainTx();
}

TEST_F(UrmaPooledTest, failed_tx_releases_context_and_fails_sender) {
    auto* sender = Endpoint();
    auto* receiver = Endpoint();
    ASSERT_NE(nullptr, sender);
    ASSERT_NE(nullptr, receiver);
    Connect(sender, receiver);
    butil::IOBuf data;
    data.append("x");
    butil::IOBuf* input[] = {&data};
    ASSERT_EQ(1, sender->CutFromIOBufList(input, 1));
    for (auto* slot : _pool->io_jfs) {
        urma_cr_t cr;
        if (urma_poll_jfc(slot->jfc, 1, &cr) == 1) {
            cr.status = URMA_CR_RNR_RETRY_CNT_EXC_ERR;
            urma::UrmaEndpoint::HandleTxCompletion(slot, cr);
            EXPECT_EQ(0u, slot->sq_inflight.load());
            EXPECT_TRUE(slot->pending.empty());
        }
    }
    EXPECT_TRUE(_sockets[0]->Failed());
    DrainRx();
}

TEST_F(UrmaPooledTest, import_cache_distinguishes_address_spaces) {
    urma_rjfr_t remote{};
    remote.jfr_id = _pool->io_jfr[0]->jfr->jfr_id;
    remote.trans_mode = URMA_TM_RM;
    remote.tp_type = URMA_CTP;
    urma_token_t token{};
    auto* a = _pool->imports->Acquire(urma::GetUrmaContext(), &remote, &token);
    ++remote.jfr_id.uasid;
    auto* b = _pool->imports->Acquire(urma::GetUrmaContext(), &remote, &token);
    ASSERT_NE(nullptr, a);
    ASSERT_NE(nullptr, b);
    EXPECT_NE(a, b);
    EXPECT_EQ(2u, _pool->imports->size());
    _pool->imports->Release(a);
    _pool->imports->Release(b);
    EXPECT_EQ(0u, _pool->imports->size());
}

class PooledEchoService : public test::EchoService {
public:
    void Echo(google::protobuf::RpcController* controller,
              const test::EchoRequest* request, test::EchoResponse* response,
              google::protobuf::Closure* done) override {
        auto* cntl = static_cast<Controller*>(controller);
        response->set_message(request->message());
        cntl->response_attachment().append(cntl->request_attachment());
        done->Run();
    }
};

TEST_F(UrmaPooledTest, event_driven_rpc_binary_and_protobuf_handshakes) {
    // Use real epoll callbacks and the normal brpc handshake/message parser.
    ASSERT_TRUE(_pool->Shutdown());
    _pool.reset();
    urma::g_urma_pool = _original;
    GFLAGS_NAMESPACE::FlagSaver flags;
    PooledEchoService service;
    Server server;
    ASSERT_EQ(0, server.AddService(&service, SERVER_DOESNT_OWN_SERVICE));
    ServerOptions server_options;
    server_options.socket_mode = SOCKET_MODE_URMA;
    ASSERT_EQ(0, server.Start("127.0.0.1:0", &server_options));
    for (int version : {2, 3}) {
        urma::FLAGS_urma_client_handshake_version = version;
        Channel channel;
        ChannelOptions options;
        options.socket_mode = SOCKET_MODE_URMA;
        options.connection_type = "short";
        options.timeout_ms = 3000;
        options.max_retry = 0;
        ASSERT_EQ(0, channel.Init(server.listen_address(), &options));
        test::EchoService_Stub stub(&channel);
        for (int n = 0; n < 4; ++n) {
            test::EchoRequest request;
            test::EchoResponse response;
            request.set_message(std::string(12000 + n, char('a' + n)));
            Controller cntl;
            cntl.request_attachment().append(std::string(11000, 'z'));
            stub.Echo(&cntl, &request, &response, nullptr);
            ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
            EXPECT_EQ(request.message(), response.message());
            EXPECT_EQ(cntl.request_attachment().to_string(),
                      cntl.response_attachment().to_string());
        }
    }
    server.Stop(0);
    server.Join();
}

TEST_F(UrmaPooledTest, concurrent_pooled_and_short_handshakes_are_independent) {
    ASSERT_TRUE(_pool->Shutdown());
    _pool.reset();
    urma::g_urma_pool = _original;
    GFLAGS_NAMESPACE::FlagSaver flags;
    PooledEchoService service;
    Server server;
    ASSERT_EQ(0, server.AddService(&service, SERVER_DOESNT_OWN_SERVICE));
    ServerOptions server_options;
    server_options.socket_mode = SOCKET_MODE_URMA;
    ASSERT_EQ(0, server.Start("127.0.0.1:0", &server_options));
    for (int version : {2, 3}) {
        urma::FLAGS_urma_client_handshake_version = version;
        for (const char* connection : {"pooled", "short"}) {
            Channel channel;
            ChannelOptions options;
            options.socket_mode = SOCKET_MODE_URMA;
            options.connection_type = connection;
            options.timeout_ms = 3000;
            options.max_retry = 0;
            ASSERT_EQ(0, channel.Init(server.listen_address(), &options));
            std::atomic<int> ready{0};
            std::vector<std::thread> workers;
            for (int i = 0; i < 4; ++i) {
                workers.emplace_back([&, i] {
                    ++ready;
                    while (ready.load() != 4) { std::this_thread::yield(); }
                    test::EchoService_Stub stub(&channel);
                    for (int n = 0; n < 4; ++n) {
                        test::EchoRequest request;
                        test::EchoResponse response;
                        request.set_message(std::string(100 + n, char('a' + i)));
                        Controller cntl;
                        stub.Echo(&cntl, &request, &response, nullptr);
                        EXPECT_FALSE(cntl.Failed()) << connection << ": " << cntl.ErrorText();
                        EXPECT_EQ(request.message(), response.message());
                    }
                });
            }
            for (auto& worker : workers) { worker.join(); }
        }
    }
    server.Stop(0);
    server.Join();
}

class BlockingPooledService : public test::EchoService {
public:
    std::atomic<bool> entered{false};
    std::atomic<bool> released{false};
    void Echo(google::protobuf::RpcController* controller,
              const test::EchoRequest* request, test::EchoResponse* response,
              google::protobuf::Closure* done) override {
        if (request->message() == "wait") {
            entered = true;
            const int64_t deadline = butil::monotonic_time_us() + 2000000;
            while (!released && butil::monotonic_time_us() < deadline) {
                bthread_usleep(1000);
            }
            if (!released) { controller->SetFailed("Shared receive CQ was blocked"); }
        } else if (request->message() == "release") {
            released = true;
        }
        response->set_message(request->message());
        done->Run();
    }
};

void UrmaPooledTest::CheckBlockingCallback() {
    ASSERT_TRUE(_pool->Shutdown());
    _pool.reset();
    urma::g_urma_pool = _original;
    ASSERT_EQ(1u, _original->io_jfr.size());
    BlockingPooledService service;
    Server server;
    ASSERT_EQ(0, server.AddService(&service, SERVER_DOESNT_OWN_SERVICE));
    ServerOptions server_options;
    server_options.socket_mode = SOCKET_MODE_URMA;
    ASSERT_EQ(0, server.Start("127.0.0.1:0", &server_options));
    Channel first, second;
    ChannelOptions options;
    options.socket_mode = SOCKET_MODE_URMA;
    options.timeout_ms = 3000;
    options.max_retry = 0;
    options.connection_group = "blocking-first";
    ASSERT_EQ(0, first.Init(server.listen_address(), &options));
    options.connection_group = "blocking-second";
    ASSERT_EQ(0, second.Init(server.listen_address(), &options));
    std::thread worker([&] {
        test::EchoService_Stub stub(&first);
        test::EchoRequest request;
        test::EchoResponse response;
        Controller cntl;
        request.set_message("wait");
        stub.Echo(&cntl, &request, &response, nullptr);
        EXPECT_FALSE(cntl.Failed()) << cntl.ErrorText();
    });
    const int64_t deadline = butil::monotonic_time_us() + 3000000;
    while (!service.entered && butil::monotonic_time_us() < deadline) { usleep(1000); }
    EXPECT_TRUE(service.entered);
    test::EchoService_Stub stub(&second);
    test::EchoRequest request;
    test::EchoResponse response;
    Controller cntl;
    request.set_message("release");
    stub.Echo(&cntl, &request, &response, nullptr);
    EXPECT_FALSE(cntl.Failed()) << cntl.ErrorText();
    worker.join();
    server.Stop(0);
    server.Join();
}

TEST_F(UrmaPooledTest, blocking_callback_does_not_block_another_connection_on_same_jfr) {
    GFLAGS_NAMESPACE::FlagSaver flags;
    ASSERT_FALSE(GFLAGS_NAMESPACE::SetCommandLineOption(
        "urma_inline_last", "false").empty());
    CheckBlockingCallback();
}

TEST_F(UrmaPooledTest, inline_callback_does_not_block_another_connection_on_same_jfr) {
    GFLAGS_NAMESPACE::FlagSaver flags;
    ASSERT_FALSE(GFLAGS_NAMESPACE::SetCommandLineOption(
        "urma_inline_last", "true").empty());
    CheckBlockingCallback();
}

TEST_F(UrmaPooledTest, disconnected_connections_release_server_sockets_and_imports) {
    ASSERT_TRUE(_pool->Shutdown());
    _pool.reset();
    urma::g_urma_pool = _original;
    GFLAGS_NAMESPACE::FlagSaver flags;
    GFLAGS_NAMESPACE::SetCommandLineOption("defer_close_second", "0");
    PooledEchoService service;
    Server server;
    ASSERT_EQ(0, server.AddService(&service, SERVER_DOESNT_OWN_SERVICE));
    ServerOptions server_options;
    server_options.socket_mode = SOCKET_MODE_URMA;
    ASSERT_EQ(0, server.Start("127.0.0.1:0", &server_options));
    // Version 0 exercises the serialized TCP-fallback reader as well.
    for (int version : {0, 2, 3}) {
        urma::FLAGS_urma_client_handshake_version = version;
        for (int cycle = 0; cycle < 3; ++cycle) {
            std::vector<std::unique_ptr<Channel>> channels;
            for (int i = 0; i < 64; ++i) {
                std::unique_ptr<Channel> channel(new Channel);
                ChannelOptions options;
                options.connection_group = "close-test-" + std::to_string(i);
                options.socket_mode = version == 0 ? SOCKET_MODE_TCP : SOCKET_MODE_URMA;
                options.timeout_ms = 3000;
                options.max_retry = 0;
                ASSERT_EQ(0, channel->Init(server.listen_address(), &options));
                test::EchoService_Stub stub(channel.get());
                test::EchoRequest request;
                test::EchoResponse response;
                request.set_message("persistent connection close/reopen");
                Controller cntl;
                stub.Echo(&cntl, &request, &response, nullptr);
                ASSERT_FALSE(cntl.Failed()) << cntl.ErrorText();
                channels.emplace_back(std::move(channel));
            }
            ServerStatistics stat;
            server.GetStat(&stat);
            ASSERT_EQ(64u, stat.connection_count);
            channels.clear();
            const int64_t deadline = butil::monotonic_time_us() + 3000000;
            do {
                usleep(1000);
                server.GetStat(&stat);
            } while ((stat.connection_count || _original->imports->size()) &&
                     butil::monotonic_time_us() < deadline);
            EXPECT_EQ(0u, stat.connection_count);
            EXPECT_EQ(0u, _original->imports->size());
        }
    }
    server.Stop(0);
    server.Join();
}

// A whole shared CQ callback may retain only one parser-last message, even
// when CQEs belong to different logical connections. Its callback must observe
// the event counter released and every queue/endpoint lock unlocked.
struct InlineDeliveryProbe {
    struct Message : InputMessageBase {
        explicit Message(char c) : value(c) {}
        char value;
        void DestroyImpl() override { delete this; }
    };
    Socket* carrier = nullptr;
    SocketId last_socket = INVALID_SOCKET_ID;
    urma::UrmaIoSlot* slot = nullptr;
    bthread_t poller = 0;
    bthread_key_t key;
    int expected_tls = 123;
    int cq_tls = 456;
    std::atomic<int> next_thread{0};
    bthread_t processing_threads[4]{};
    std::atomic<int> processed{0};
    std::atomic<int> inline_count{0};
    static ParseResult Parse(butil::IOBuf* data, Socket*, bool, const void*) {
        if (data->empty()) { return MakeParseError(PARSE_ERROR_NOT_ENOUGH_DATA); }
        char value;
        data->cutn(&value, 1);
        return MakeMessage(new Message(value));
    }
    static void Process(InputMessageBase* message) {
        auto* probe = const_cast<InlineDeliveryProbe*>(
            static_cast<const InlineDeliveryProbe*>(message->arg()));
        const int index = probe->next_thread.fetch_add(1);
        EXPECT_LT(index, 4);
        if (index < 4) { probe->processing_threads[index] = bthread_self(); }
        EXPECT_EQ(&probe->expected_tls, bthread_getspecific(probe->key));
        if (bthread_self() == probe->poller) {
            ++probe->inline_count;
            EXPECT_EQ(probe->last_socket, message->socket()->id());
            EXPECT_EQ('b', static_cast<Message*>(message)->value);
            EXPECT_EQ(0, probe->carrier->_nevent.load());
            auto* endpoint = static_cast<UrmaTransport*>(
                message->socket()->_transport.get())->GetUrmaEp();
            for (auto* mutex : {&probe->slot->mutex, &endpoint->_dispatch_mutex,
                                &endpoint->_reorder_mutex}) {
                const bool unlocked = mutex->try_lock();
                EXPECT_TRUE(unlocked);
                if (unlocked) { mutex->unlock(); }
            }
        }
        message->Destroy();
        ++probe->processed;
    }
};

TEST_F(UrmaPooledTest, inline_batch_releases_locks_and_event_once_for_all_connections) {
    GFLAGS_NAMESPACE::FlagSaver flags;
    GFLAGS_NAMESPACE::SetCommandLineOption("urma_inline_last", "true");
    InlineDeliveryProbe probe;
    // Keep one worker from taking all eight pre-reserved tables into its
    // pthread-local cache before the other message workers can borrow one.
    GFLAGS_NAMESPACE::SetCommandLineOption("borrow_from_globle_size", "1");
    bthread_keytable_pool_t key_pool;
    ASSERT_EQ(0, bthread_keytable_pool_init(&key_pool));
    ASSERT_EQ(0, bthread_key_create(&probe.key, nullptr));
    bthread_keytable_pool_reserve(&key_pool, 8, probe.key,
        [](const void* p) -> void* { return const_cast<void*>(p); }, &probe.expected_tls);
    InputMessenger messenger;
    InputMessageHandler handler = {InlineDeliveryProbe::Parse,
        InlineDeliveryProbe::Process, nullptr, &probe, "inline-probe"};
    ASSERT_EQ(0, messenger.AddNonProtocolHandler(handler));
    SocketOptions carrier_options;
    probe.slot = _pool->io_jfr[0];
    carrier_options.user = probe.slot;
    probe.slot->AddRef();
    SocketId id;
    ASSERT_EQ(0, Socket::Create(carrier_options, &id));
    SocketUniquePtr carrier;
    ASSERT_EQ(0, Socket::Address(id, &carrier));
    probe.carrier = carrier.get();
    probe.poller = bthread_self();
    carrier->_nevent.store(1);
    for (int connection = 0; connection < 2; ++connection) {
        auto* sender = Endpoint();
        auto* receiver = Endpoint();
        ASSERT_TRUE(sender && receiver);
        receiver->_socket->_user = &messenger;
        receiver->_socket->_keytable_pool = &key_pool;
        probe.last_socket = receiver->_socket->id();
        receiver->_state = urma::UrmaEndpoint::ESTABLISHED;
        Connect(sender, receiver);
        // Two parsed messages per CQE: the first stays asynchronous.
        butil::IOBuf data;
        data.append("ab", 2);
        butil::IOBuf* list[] = {&data};
        ASSERT_EQ(2, sender->CutFromIOBufList(list, 1));
    }
    bthread_t poll_thread;
    ASSERT_EQ(0, bthread_start_background(&poll_thread, nullptr,
        [](void* p) -> void* {
            auto* probe = static_cast<InlineDeliveryProbe*>(p);
            probe->poller = bthread_self();
            EXPECT_EQ(0, bthread_setspecific(probe->key, &probe->cq_tls));
            urma::UrmaEndpoint::PollIoJfc(probe->carrier);
            EXPECT_EQ(&probe->cq_tls, bthread_getspecific(probe->key));
            return nullptr;
        }, &probe));
    ASSERT_EQ(0, bthread_join(poll_thread, nullptr));
    const int64_t deadline = butil::monotonic_time_us() + 2000000;
    while (probe.processed != 4 && butil::monotonic_time_us() < deadline) {
        usleep(1000);
    }
    EXPECT_EQ(4, probe.processed.load());
    EXPECT_EQ(1, probe.inline_count.load());
    for (bthread_t thread : probe.processing_threads) {
        if (thread && thread != probe.poller) { EXPECT_EQ(0, bthread_join(thread, nullptr)); }
    }
    for (auto& socket : _sockets) {
        socket->_user = nullptr;
        socket->_keytable_pool = nullptr;
    }
    ASSERT_EQ(0, bthread_keytable_pool_destroy(&key_pool));
    ASSERT_EQ(0, bthread_key_delete(probe.key));
    carrier->SetFailed();
    DrainTx();
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
