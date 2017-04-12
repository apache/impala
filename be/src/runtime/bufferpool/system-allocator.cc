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

#include "runtime/bufferpool/system-allocator.h"

#include <sys/mman.h>

#include "util/bit-util.h"

// TODO: IMPALA-5073: this should eventually become the default once we are confident
// that it is superior to allocating via TCMalloc.
DEFINE_bool(mmap_buffers, false,
    "(Advanced) If true, allocate buffers directly from the operating system instead of "
    "with TCMalloc.");

DEFINE_bool(madvise_huge_pages, true,
    "(Advanced) If true and --mmap_buffers is also "
    "true, advise operating system to back large memory buffers with huge pages");

namespace impala {

/// This is the huge page size on x86-64. We could parse /proc/meminfo to programmatically
/// get this, but it is unlikely to change unless we port to a different architecture.
static int64_t HUGE_PAGE_SIZE = 2LL * 1024 * 1024;

SystemAllocator::SystemAllocator(int64_t min_buffer_len)
  : min_buffer_len_(min_buffer_len) {
  DCHECK(BitUtil::IsPowerOf2(min_buffer_len));
}

Status SystemAllocator::Allocate(int64_t len, BufferPool::BufferHandle* buffer) {
  DCHECK_GE(len, min_buffer_len_);
  DCHECK_LE(len, BufferPool::MAX_BUFFER_BYTES);
  DCHECK(BitUtil::IsPowerOf2(len)) << len;

  uint8_t* buffer_mem;
  if (FLAGS_mmap_buffers) {
    RETURN_IF_ERROR(AllocateViaMMap(len, &buffer_mem));
  } else {
    // AddressSanitizer does not instrument mmap(). Use malloc() to preserve
    // instrumentation.
    buffer_mem = reinterpret_cast<uint8_t*>(malloc(len));
    if (buffer_mem == nullptr) {
      return Status(
          TErrorCode::BUFFER_ALLOCATION_FAILED, len, "malloc() failed under asan");
    }
  }
  buffer->Open(buffer_mem, len, CpuInfo::GetCurrentCore());
  return Status::OK();
}

Status SystemAllocator::AllocateViaMMap(int64_t len, uint8_t** buffer_mem) {
  int64_t map_len = len;
  bool use_huge_pages = len % HUGE_PAGE_SIZE == 0 && FLAGS_madvise_huge_pages;
  if (use_huge_pages) {
    // Map an extra huge page so we can fix up the alignment if needed.
    map_len += HUGE_PAGE_SIZE;
  }
  uint8_t* mem = reinterpret_cast<uint8_t*>(
      mmap(nullptr, map_len, PROT_READ | PROT_WRITE, MAP_ANONYMOUS | MAP_PRIVATE, -1, 0));
  if (mem == MAP_FAILED) {
    const char* error = strerror(errno);
    return Status(TErrorCode::BUFFER_ALLOCATION_FAILED, len, error);
  }

  if (use_huge_pages) {
    // mmap() may return memory that is not aligned to the huge page size. For the
    // subsequent madvise() call to work well, we need to align it ourselves and
    // unmap the memory on either side of the buffer that we don't need.
    uintptr_t misalignment = reinterpret_cast<uintptr_t>(mem) % HUGE_PAGE_SIZE;
    if (misalignment != 0) {
      uintptr_t fixup = HUGE_PAGE_SIZE - misalignment;
      munmap(mem, fixup);
      mem += fixup;
      map_len -= fixup;
    }
    munmap(mem + len, map_len - len);
    DCHECK_EQ(reinterpret_cast<uintptr_t>(mem) % HUGE_PAGE_SIZE, 0) << mem;
    // Mark the buffer as a candidate for promotion to huge pages. The Linux Transparent
    // Huge Pages implementation will try to back the memory with a huge page if it is
    // enabled. MADV_HUGEPAGE was introduced in 2.6.38, so we similarly need to skip this
    // code if we are compiling against an older kernel.
#ifdef MADV_HUGEPAGE
    int rc;
    // According to madvise() docs it may return EAGAIN to signal that we should retry.
    do {
      rc = madvise(mem, len, MADV_HUGEPAGE);
    } while (rc == -1 && errno == EAGAIN);
    DCHECK(rc == 0) << "madvise(MADV_HUGEPAGE) shouldn't fail" << errno;
#endif
  }
  *buffer_mem = mem;
  return Status::OK();
}

void SystemAllocator::Free(BufferPool::BufferHandle&& buffer) {
  if (FLAGS_mmap_buffers) {
    int rc = munmap(buffer.data(), buffer.len());
    DCHECK_EQ(rc, 0) << "Unexpected munmap() error: " << errno;
  } else {
    free(buffer.data());
  }
  buffer.Reset(); // Avoid DCHECK in ~BufferHandle().
}
}
