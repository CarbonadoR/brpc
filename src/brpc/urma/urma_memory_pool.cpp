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
#include <algorithm>
#include <sys/mman.h>
#include <pthread.h>
#include <atomic>
#include <cerrno>
#include <cstdlib>
#include <functional>
#include <limits>
#include <new>
#include <unordered_map>
#include <vector>
#include <gflags/gflags.h>
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
DEFINE_bool(urma_header_tls_cache, true,
            "Cache registered 64-byte URMA control headers per pthread");
namespace {
const size_t kShards = 64;
const size_t kHeaderCacheLimit = 256;
const size_t kHeaderCacheRefill = 32;
const size_t kMaxHeaderThreadCaches = 128;
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
    size_t AllocateBatch(size_t size, void** buffers, size_t capacity) override {
        if (!_seg) { errno = ENODEV; return 0; }
        if (!buffers || !capacity || size > _block_size) {
            errno = EMSGSIZE;
            return 0;
        }
        const size_t start = std::hash<pthread_t>()(pthread_self()) % kShards;
        size_t count = 0;
        for (size_t i = 0; i < kShards && count < capacity; ++i) {
            const size_t shard = (start + i) % kShards;
            BAIDU_SCOPED_LOCK(_mutex[shard]);
            while (!_free[shard].empty() && count < capacity) {
                void* p = _free[shard].back();
                _free[shard].pop_back();
                _used[Index(p)] = 1;
                _outstanding.fetch_add(1, std::memory_order_relaxed);
                buffers[count++] = p;
            }
        }
        if (!count) { errno = ENOMEM; }
        return count;
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

// Header buffers are returned to the freeing pthread's bounded cache. A
// refill/flush touches the shared registered pool; cache hits avoid its shard
// locks. Cache entries remain outstanding in the backing pool until flushed.
class CachedHeaderPool final : public UrmaMemoryPool {
    struct ThreadCache {
        std::vector<void*> buffers;
    };
public:
    CachedHeaderPool(std::unique_ptr<UrmaMemoryPool> backing, size_t pool_count)
        : _backing(std::move(backing)),
          _max_total_cached(std::min(kHeaderCacheLimit * kMaxHeaderThreadCaches,
                  std::max<size_t>(1, pool_count / 8))),
          _id(s_next_id.fetch_add(1, std::memory_order_relaxed)) {}
    ~CachedHeaderPool() override { FlushCaches(); }

    void* Allocate(size_t size) override {
        if (size > 64) { errno = EMSGSIZE; return nullptr; }
        if (!_registered.load(std::memory_order_acquire)) {
            errno = ENODEV;
            return nullptr;
        }
        ThreadCache* cache = GetThreadCache();
        if (!cache) {
            void* p = _backing->Allocate(size);
            if (p) { _outstanding.fetch_add(1, std::memory_order_relaxed); }
            return p;
        }
        if (cache->buffers.empty()) {
            const size_t cached = _cached_blocks.load(std::memory_order_relaxed);
            const size_t budget = cached < _max_total_cached
                ? _max_total_cached - cached : 0;
            if (!budget) {
                void* p = _backing->Allocate(size);
                if (p) { _outstanding.fetch_add(1, std::memory_order_relaxed); }
                return p;
            }
            void* fetched[kHeaderCacheRefill];
            const size_t want = std::min(kHeaderCacheRefill, budget);
            const size_t count = _backing->AllocateBatch(size, fetched, want);
            if (!count) { return nullptr; }
            for (size_t i = 0; i < count; ++i) { cache->buffers.push_back(fetched[i]); }
            const size_t cached_after = _cached_blocks.fetch_add(
                count, std::memory_order_relaxed) + count;
            if (cached_after > _max_total_cached) {
                FlushSome(cache, std::min(cached_after - _max_total_cached,
                                          cache->buffers.size()));
            }
        }
        if (cache->buffers.empty()) {
            void* p = _backing->Allocate(size);
            if (p) { _outstanding.fetch_add(1, std::memory_order_relaxed); }
            return p;
        }
        void* p = cache->buffers.back();
        cache->buffers.pop_back();
        _cached_blocks.fetch_sub(1, std::memory_order_relaxed);
        _outstanding.fetch_add(1, std::memory_order_relaxed);
        return p;
    }
    void Deallocate(void* p) override {
        if (!p) { return; }
        CHECK(_backing->Owns(p));
        const size_t old = _outstanding.fetch_sub(1, std::memory_order_relaxed);
        CHECK_GT(old, 0u);
        ThreadCache* cache = GetThreadCache();
        if (!cache || !_registered.load(std::memory_order_acquire)) {
            _backing->Deallocate(p);
            return;
        }
        if (cache->buffers.size() == kHeaderCacheLimit) {
            FlushSome(cache, std::max<size_t>(1, cache->buffers.size() / 2));
        }
        cache->buffers.push_back(p);
        const size_t cached = _cached_blocks.fetch_add(
            1, std::memory_order_relaxed) + 1;
        if (cached > _max_total_cached) {
            FlushSome(cache, std::min(cached - _max_total_cached,
                                      cache->buffers.size()));
        }
    }
    urma_target_seg_t* SegmentFor(const void* p) const override {
        return _backing->SegmentFor(p);
    }
    bool Owns(const void* p) const override { return _backing->Owns(p); }
    size_t outstanding() const override {
        return _outstanding.load(std::memory_order_relaxed);
    }
    bool Unregister() override {
        if (!FlushCaches() || !_backing->Unregister()) { return false; }
        _registered.store(false, std::memory_order_release);
        return true;
    }
    bool FlushCaches() override {
        // Caller guarantees all alloc/free users have quiesced.
        BAIDU_SCOPED_LOCK(_registry_mutex);
        for (const auto& cache : _caches) {
            FlushSome(cache.get(), cache->buffers.size());
        }
        return _cached_blocks.load(std::memory_order_relaxed) == 0;
    }
private:
    ThreadCache* GetThreadCache() {
        thread_local std::unordered_map<uint64_t, ThreadCache*> local;
        auto it = local.find(_id);
        if (it != local.end()) { return it->second; }
        {
            BAIDU_SCOPED_LOCK(_registry_mutex);
            if (_caches.size() >= kMaxHeaderThreadCaches) {
                local.emplace(_id, nullptr);
                return nullptr;
            }
        }
        std::unique_ptr<ThreadCache> cache(new (std::nothrow) ThreadCache);
        if (!cache) {
            local.emplace(_id, nullptr);
            return nullptr;
        }
        cache->buffers.reserve(kHeaderCacheLimit);
        ThreadCache* result = cache.get();
        {
            BAIDU_SCOPED_LOCK(_registry_mutex);
            if (_caches.size() >= kMaxHeaderThreadCaches) {
                local.emplace(_id, nullptr);
                return nullptr;
            }
            _caches.push_back(std::move(cache));
        }
        local.emplace(_id, result);
        return result;
    }
    void FlushSome(ThreadCache* cache, size_t count) {
        if (!cache || !count) { return; }
        count = std::min(count, cache->buffers.size());
        while (count) {
            void* p = cache->buffers.back();
            cache->buffers.pop_back();
            _backing->Deallocate(p);
            _cached_blocks.fetch_sub(1, std::memory_order_relaxed);
            --count;
        }
    }
    std::unique_ptr<UrmaMemoryPool> _backing;
    const size_t _max_total_cached;
    const uint64_t _id;
    butil::Mutex _registry_mutex;
    std::vector<std::unique_ptr<ThreadCache>> _caches;
    std::atomic<size_t> _outstanding{0};
    std::atomic<size_t> _cached_blocks{0};
    std::atomic<bool> _registered{true};
    static std::atomic<uint64_t> s_next_id;
};
std::atomic<uint64_t> CachedHeaderPool::s_next_id{1};

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
    if (FLAGS_urma_header_tls_cache) {
        std::unique_ptr<UrmaMemoryPool> cached(new (std::nothrow)
            CachedHeaderPool(std::move(header), headers));
        if (!cached) { errno = ENOMEM; return false; }
        header = std::move(cached);
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
        if (!g_header_pool->FlushCaches() || g_header_pool->outstanding() != 0 ||
            !g_header_pool->Unregister()) {
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
