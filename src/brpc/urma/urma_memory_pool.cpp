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

#include "brpc/urma/urma_memory_pool.h"

#if BRPC_WITH_URMA
#include <sys/mman.h>
#include <pthread.h>
#include <atomic>
#include <cerrno>
#include <cstdlib>
#include <functional>
#include <limits>
#include <vector>
#include "butil/iobuf.h"
#include "butil/logging.h"
#include "butil/scoped_lock.h"
#include "butil/synchronization/lock.h"

namespace butil {
namespace iobuf {
extern void* (*blockmem_allocate)(size_t);
extern void (*blockmem_deallocate)(void*);
}
}

namespace brpc {
namespace urma {
namespace {
const size_t kShards = 64;
class RegisteredPool : public UrmaMemoryPool {
public:
    RegisteredPool(size_t size, size_t count)
        : _block_size(size), _count(count) {}
    ~RegisteredPool() override {
        // Never invalidate DMA or application-owned blocks on a failed cleanup.
        if (_outstanding.load() == 0 && Unregister() && _base) {
            munmap(_base, _block_size * _count);
        }
    }
    bool Init(urma_context_t* context) {
        if (!context || !_count || _block_size < sizeof(void*) ||
            _block_size % alignof(void*) ||
            _count > std::numeric_limits<size_t>::max() / _block_size) {
            errno = EINVAL;
            return false;
        }
        _base = mmap(nullptr, _block_size * _count, PROT_READ | PROT_WRITE,
                     MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
        if (_base == MAP_FAILED) {
            _base = nullptr;
            return false;
        }
        urma_seg_cfg_t cfg{};
        cfg.va = reinterpret_cast<uint64_t>(_base);
        cfg.len = _block_size * _count;
        cfg.flag.bs.token_policy = URMA_TOKEN_NONE;
        cfg.flag.bs.cacheable = URMA_NON_CACHEABLE;
        cfg.flag.bs.access = URMA_ACCESS_READ | URMA_ACCESS_WRITE |
                             URMA_ACCESS_ATOMIC;
        _seg = urma_register_seg(context, &cfg);
        if (!_seg) {
            return false;
        }
        _used.assign(_count, 0);
        for (size_t i = 0; i < _count; ++i) {
            _free[i % kShards].push_back(static_cast<char*>(_base) +
                                          i * _block_size);
        }
        return true;
    }
    void* Allocate(size_t size) override {
        if (!_seg) { errno = ENODEV; return nullptr; }
        if (size > _block_size) {
            errno = EMSGSIZE;
            return nullptr;
        }
        const size_t start = std::hash<pthread_t>()(pthread_self()) % kShards;
        for (size_t i = 0; i < kShards; ++i) {
            const size_t shard = (start + i) % kShards;
            BAIDU_SCOPED_LOCK(_mutex[shard]);
            if (_free[shard].empty()) {
                continue;
            }
            void* p = _free[shard].back();
            _free[shard].pop_back();
            _used[Index(p)] = 1;
            _outstanding.fetch_add(1, std::memory_order_relaxed);
            return p;
        }
        errno = ENOMEM;
        return nullptr;
    }
    void Deallocate(void* p) override {
        if (!p) {
            return;
        }
        CHECK(Owns(p));
        CHECK_EQ((reinterpret_cast<uintptr_t>(p) -
                  reinterpret_cast<uintptr_t>(_base)) % _block_size, 0u);
        const size_t index = Index(p);
        BAIDU_SCOPED_LOCK(_mutex[index % kShards]);
        CHECK(_used[index]);
        _used[index] = 0;
        _free[index % kShards].push_back(p);
        _outstanding.fetch_sub(1, std::memory_order_relaxed);
    }
    bool Owns(const void* p) const override {
        const uintptr_t base = reinterpret_cast<uintptr_t>(_base);
        const uintptr_t addr = reinterpret_cast<uintptr_t>(p);
        return _base && addr >= base && addr - base < _block_size * _count;
    }
    urma_target_seg_t* SegmentFor(const void* p) const override {
        return !p || Owns(p) ? _seg : nullptr;
    }
    size_t outstanding() const override { return _outstanding.load(); }
    bool Unregister() override {
        if (_seg && urma_unregister_seg(_seg) != URMA_SUCCESS) {
            return false;
        }
        _seg = nullptr;
        return true;
    }
private:
    size_t Index(const void* p) const {
        return (reinterpret_cast<uintptr_t>(p) -
                reinterpret_cast<uintptr_t>(_base)) / _block_size;
    }
    size_t _block_size;
    size_t _count;
    void* _base{nullptr};
    urma_target_seg_t* _seg{nullptr};
    butil::Mutex _mutex[kShards];
    std::vector<void*> _free[kShards];
    std::vector<char> _used;
    std::atomic<size_t> _outstanding{0};
};

// These allocators intentionally live until process exit: IOBuf has TLS block
// caches. On rollback unregister DMA but retain mappings and the deallocator
// while any application/TLS block still refers to them.
UrmaMemoryPoolFactory g_factory = CreateUrmaMemoryPool;
UrmaMemoryPool* g_data_pool = nullptr;
UrmaMemoryPool* g_header_pool = nullptr;
void* (*g_original_allocate)(size_t) = nullptr;
void (*g_original_deallocate)(void*) = nullptr;
size_t g_original_block_size = 0;
void* AllocateIOBuf(size_t size) {
    void* p = g_data_pool->Allocate(size);
    return p ? p : g_original_allocate(size);
}
void DeallocateIOBuf(void* p) {
    if (g_data_pool->Owns(p)) {
        g_data_pool->Deallocate(p);
    } else {
        g_original_deallocate(p);
    }
}
}  // namespace

std::unique_ptr<UrmaMemoryPool> CreateUrmaMemoryPool(
        urma_context_t* context, size_t size, size_t count) {
    std::unique_ptr<RegisteredPool> pool(new RegisteredPool(size, count));
    if (!pool->Init(context)) {
        return nullptr;
    }
    return std::unique_ptr<UrmaMemoryPool>(pool.release());
}

bool SetUrmaMemoryPoolFactory(UrmaMemoryPoolFactory factory) {
    if (g_data_pool || g_header_pool) { errno = EBUSY; return false; }
    g_factory = factory ? factory : CreateUrmaMemoryPool;
    return true;
}
bool InitializeUrmaMemoryPools(urma_context_t* context, size_t size,
                               size_t count, size_t headers) {
    if (g_data_pool || g_header_pool) {
        errno = EALREADY;
        return false;
    }
    auto data = g_factory(context, size, count);
    auto header = g_factory(context, 64, headers);
    if (!data || !header) {
        return false;
    }
    return InstallUrmaMemoryPools(std::move(data), std::move(header), size);
}

bool InstallUrmaMemoryPools(std::unique_ptr<UrmaMemoryPool> data,
                            std::unique_ptr<UrmaMemoryPool> header, size_t size) {
    if (g_data_pool || g_header_pool || !data || !header ||
        size <= sizeof(butil::IOBuf::Block)) {
        errno = EINVAL;
        return false;
    }
    g_data_pool = data.release();
    g_header_pool = header.release();
    g_original_allocate = butil::iobuf::blockmem_allocate;
    g_original_deallocate = butil::iobuf::blockmem_deallocate;
    g_original_block_size = butil::GetDefaultBlockSize();
    butil::iobuf::blockmem_allocate = AllocateIOBuf;
    butil::iobuf::blockmem_deallocate = DeallocateIOBuf;
    butil::SetDefaultBlockSize(size);
    return true;
}

bool ReleaseUrmaMemoryPools() {
    if (g_header_pool) {
        if (g_header_pool->outstanding() != 0 || !g_header_pool->Unregister()) {
            return false;
        }
        delete g_header_pool;
        g_header_pool = nullptr;
    }
    if (g_data_pool) {
        if (!g_data_pool->Unregister()) { return false; }
        butil::SetDefaultBlockSize(g_original_block_size);
        if (g_data_pool->outstanding() == 0) {
            butil::iobuf::blockmem_allocate = g_original_allocate;
            butil::iobuf::blockmem_deallocate = g_original_deallocate;
            delete g_data_pool;
            g_data_pool = nullptr;
        }
    }
    return true;
}
void* AllocHeaderBuffer() {
    return g_header_pool ? g_header_pool->Allocate(64) : nullptr;
}
void FreeHeaderBuffer(void* p) {
    if (p) { g_header_pool->Deallocate(p); }
}
urma_target_seg_t* GetHeaderPoolSegFor(void* p) {
    return g_header_pool ? g_header_pool->SegmentFor(p) : nullptr;
}
urma_target_seg_t* GetPoolSegFor(void* p) {
    return g_data_pool ? g_data_pool->SegmentFor(p) : nullptr;
}
}  // namespace urma
}  // namespace brpc
#endif
