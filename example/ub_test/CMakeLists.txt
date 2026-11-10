# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

cmake_minimum_required(VERSION 3.16...3.28)
project(ub_test C CXX)

include(${CMAKE_CURRENT_LIST_DIR}/../cmake/BrpcExample.cmake)

option(LINK_SO "Whether examples are linked dynamically" OFF)

brpc_example_find_common_deps(DYNAMIC_LIB)
find_library(TCMALLOC_LIB NAMES tcmalloc)
get_filename_component(TCMALLOC_LIB_DIR "${TCMALLOC_LIB}" DIRECTORY)
find_path(TCMALLOC_INCLUDE_PATH NAMES gperftools/malloc_extension.h
    HINTS "${TCMALLOC_LIB_DIR}/../include")
if(NOT TCMALLOC_LIB OR NOT TCMALLOC_INCLUDE_PATH)
    message(FATAL_ERROR "URMA examples require libtcmalloc and gperftools headers")
endif()
list(APPEND DYNAMIC_LIB ${TCMALLOC_LIB})

protobuf_generate_cpp(PROTO_SRC PROTO_HEADER test.proto)
set(BRPC_EXAMPLE_WITH_URMA ON)
find_path(URMA_INCLUDE_PATH NAMES urma_api.h
    HINTS ENV URMA_ROOT
    PATHS /usr/include /usr/local/include
    PATH_SUFFIXES ub/umdk/urma umdk/urma urma
                  src/urma/lib/urma/core/include)
if(NOT URMA_INCLUDE_PATH)
    message(FATAL_ERROR
        "Fail to find urma_api.h. Install UMDK headers or set URMA_ROOT.")
endif()
find_library(URMA_LIB NAMES urma
    HINTS ENV URMA_ROOT
    PATH_SUFFIXES lib lib64)
if(URMA_LIB)
    list(APPEND DYNAMIC_LIB ${URMA_LIB})
else()
    message(STATUS
        "liburma not found; using the URMA implementation linked into brpc")
endif()

# Match the original matrix's BUTIL_USE_CPU_FREQUENCY=true build.
option(WITH_CPU_FREQUENCY "Use CPU frequency for benchmark timing" ON)
foreach(role client server)
    add_executable(ub_test_${role} ${role}.cpp ${PROTO_SRC} ${PROTO_HEADER})
    brpc_example_configure_target(ub_test_${role})
    target_compile_definitions(ub_test_${role} PRIVATE
        BUTIL_USE_CPU_FREQUENCY=$<BOOL:${WITH_CPU_FREQUENCY}>)
    target_link_libraries(ub_test_${role} PRIVATE ${BRPC_LIB} ${DYNAMIC_LIB})
    set_target_properties(ub_test_${role} PROPERTIES
        BUILD_WITH_INSTALL_RPATH TRUE
        INSTALL_RPATH "$ORIGIN;$ORIGIN/../common")
endforeach()
