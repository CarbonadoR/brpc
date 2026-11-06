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

#ifndef BRPC_URMA_MEMORY_POOL_H
#define BRPC_URMA_MEMORY_POOL_H

#if BRPC_WITH_URMA
#include <cstddef>
#include <memory>
#include "urma_api.h"

namespace brpc {
namespace urma {

// Replacement boundary for registered storage. Allocate never returns an
// unregistered buffer. The owner must stop DMA before Unregister/ destruction.
class UrmaMemoryPool {
public:
    virtual ~UrmaMemoryPool() = default;
    virtual void* Allocate(size_t size) = 0;
    virtual void Deallocate(void* buffer) = 0;
    virtual urma_target_seg_t* SegmentFor(const void* address) const = 0;
    virtual bool Owns(const void* address) const = 0;
    virtual size_t outstanding() const = 0;
    virtual bool Unregister() = 0;
};

// Fixed-size, sharded, registered mmap allocator; also used for raw 64B
// headers. Headers never enter the IOBuf block allocator.
std::unique_ptr<UrmaMemoryPool> CreateUrmaMemoryPool(
    urma_context_t* context, size_t block_size, size_t block_count);

// Install alternative allocators before URMA initialization. Ownership moves to
// the process; the data allocator's block size becomes IOBuf's default size.
bool InstallUrmaMemoryPools(std::unique_ptr<UrmaMemoryPool> data,
                            std::unique_ptr<UrmaMemoryPool> headers,
                            size_t block_size);
typedef std::unique_ptr<UrmaMemoryPool> (*UrmaMemoryPoolFactory)(
    urma_context_t*, size_t, size_t);
// Call before global initialization; nullptr restores the default allocator.
bool SetUrmaMemoryPoolFactory(UrmaMemoryPoolFactory factory);
bool InitializeUrmaMemoryPools(urma_context_t* context,
                               size_t block_size, size_t block_count,
                               size_t header_count);
// Returns false if a registered segment must remain alive.
bool ReleaseUrmaMemoryPools();
void* AllocHeaderBuffer();
void FreeHeaderBuffer(void* buffer);
urma_target_seg_t* GetHeaderPoolSegFor(void* buffer);
urma_target_seg_t* GetPoolSegFor(void* buffer);

}  // namespace urma
}  // namespace brpc
#endif
#endif
