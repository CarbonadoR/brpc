// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information.
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy at http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.

#ifndef EXAMPLE_UB_TEST_SGE_MODE_H
#define EXAMPLE_UB_TEST_SGE_MODE_H

#include <iostream>
#include <string>
#include "brpc/urma/urma_helper.h"
#include "brpc/urma/urma_resource_pool.h"

// Inspect the loaded library once, before starting benchmark traffic. Selecting
// a mode means loading its libbrpc build, not changing the hardware SGE limit.
inline bool ValidateUrmaSgeMode(const std::string& requested) {
    auto* pool = brpc::urma::g_urma_pool;
    if (!pool || pool->io_jfs.empty() || !pool->io_jfs.front() ||
        !pool->io_jfs.front()->jfs) {
        std::cerr << "URMA resource pool is unavailable; refusing TCP fallback"
                  << std::endl;
        return false;
    }
    const uint32_t max_sge = pool->io_jfs.front()->jfs->jfs_cfg.max_sge;
    const std::string actual = max_sge == 1 ? "single" : "multi";
    if (requested != "auto" && requested != actual) {
        std::cerr << "Requested SGE mode " << requested << ", loaded " << actual
                  << "; select the matching libbrpc build" << std::endl;
        return false;
    }
    auto* context = brpc::urma::GetUrmaContext();
    urma_device_attr_t attr = {};
    if (!context || urma_query_device(context->dev, &attr) != URMA_SUCCESS) {
        std::cerr << "Cannot verify URMA device CTP capabilities" << std::endl;
        return false;
    }
    const int priority = brpc::urma::FindUrmaPriorityForTpType(attr, URMA_CTP);
    for (const auto* slot : pool->io_jfs) {
        if (priority < 0 || !slot || !slot->jfs ||
            slot->jfs->jfs_cfg.max_sge != max_sge ||
            slot->jfs->jfs_cfg.priority != priority) {
            std::cerr << "Inconsistent JFS SGE configuration or non-CTP transport"
                      << std::endl;
            return false;
        }
    }
    std::cout << "UB_TEST_TRANSPORT sge_mode=" << actual
              << " max_sge=" << max_sge << " tp=CTP priority=" << priority
              << " jfs_count=" << pool->io_jfs.size()
              << " jfr_count=" << pool->io_jfr.size()
              << std::endl;
    return true;
}

#endif  // EXAMPLE_UB_TEST_SGE_MODE_H
