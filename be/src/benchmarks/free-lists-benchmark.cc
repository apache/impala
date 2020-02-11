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

#include <math.h>
#include <random>
#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <utility>
#include <vector>

#include <boost/thread/thread.hpp>

#include "common/object-pool.h"
#include "gutil/strings/substitute.h"
#include "runtime/bufferpool/free-list.h"
#include "runtime/bufferpool/system-allocator.h"
#include "util/aligned-new.h"
#include "util/benchmark.h"
#include "util/cpu-info.h"
#include "util/mem-range.h"
#include "util/stopwatch.h"

#include "common/names.h"

using namespace impala;
using boost::thread;
using boost::thread_group;
using std::mt19937_64;
using std::uniform_int_distribution;
using strings::Substitute;

// This benchmark tests free list performance under concurrency by measuring the
// throughput of a batch of Allocate()/Free() operations that use the free list
// to cache buffers. The benchmark measures total throughput across all threads.
// The baseline is the throughput of a single thread that is not competing for any
// resources.
//
// These Allocate() and Free() calls are interleaved with work that touches the
// allocated memory, so that the benchmark exhibits a realistic level of lock
// contention.
//
// The buffer size is varied from 64KB to 256KB (at 256KB, throughput is already limited
// by memory bandwidth instead of the free lists so there is little value in collecting
// more data points) and the number of iterations ("iters") over the allocated memory is
// varied.
//
// Summary of results:
// -------------------
// In the 0 iters case, which measures pure throughput of free list operations,
// ListPerThread has much higher throughput than SharedList because there is no
// contention over locks. NoList measures performance of the underlying system
// allocator (TCMalloc). TCMalloc performs poorly on large buffers compared to
// ListPerThread because it must frequently access the shared global heap and
// contend for a single global lock to serve large allocations.
//
// Performance of all three free list approaches (SharedList, ListPerThread, NoList)
// converges for larger buffer sizes and # of iterations. Performance per core also
// drops off as core count increases, regardless of whether the threads are contending
// over locks. This suggests that typical workloads that do some non-trivial processing
// over allocated buffers >= 64KB will usually be bottlenecked by memory access instead
// of the allocator. However, the balance may shift on larger machines with more cores
// as lock contention over a global mutex would become very expensive. Thus, sharing a
// free list between a small number of cores may work reasonably in practice, but sharing
// one between all cores will likely not scale at some point.
//
/*
Machine Info: Intel(R) Core(TM) i7-4790 CPU @ 3.60GHz

FreeLists 0 iters on 64 kb:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
                                                                         (relative) (relative) (relative)
---------------------------------------------------------------------------------------------------------
               1 threads SharedList               24.6       25     25.3         1X         1X         1X
               2 threads SharedList               9.05     9.53     10.6     0.368X     0.381X      0.42X
               4 threads SharedList               7.96     8.18     8.41     0.324X     0.327X     0.332X
               8 threads SharedList               6.37      6.5     6.72     0.259X      0.26X     0.265X
            1 threads ListPerThread               24.4       25     25.3     0.993X     0.998X     0.999X
            2 threads ListPerThread               33.9     35.6     36.6      1.38X      1.42X      1.45X
            4 threads ListPerThread               40.5     45.9     50.4      1.65X      1.83X      1.99X
            8 threads ListPerThread               38.1     41.9     44.5      1.55X      1.67X      1.76X
                   1 threads NoList               33.1     34.5     35.1      1.35X      1.38X      1.38X
                   2 threads NoList               42.5     43.9     45.2      1.73X      1.75X      1.79X
                   4 threads NoList               34.7     36.8     39.6      1.41X      1.47X      1.56X
                   8 threads NoList               25.3     27.6     29.6      1.03X       1.1X      1.17X

FreeLists 0 iters on 128 kb:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
                                                                         (relative) (relative) (relative)
---------------------------------------------------------------------------------------------------------
               1 threads SharedList               19.9     20.2     20.7         1X         1X         1X
               2 threads SharedList               8.85     9.35     10.3     0.445X     0.463X     0.496X
               4 threads SharedList               7.72     7.93     8.09     0.388X     0.393X      0.39X
               8 threads SharedList               6.21     6.38     6.54     0.312X     0.316X     0.315X
            1 threads ListPerThread               19.8     20.2     20.7     0.997X         1X         1X
            2 threads ListPerThread                 30     31.3     32.2      1.51X      1.55X      1.55X
            4 threads ListPerThread               34.9     36.9     40.6      1.75X      1.83X      1.96X
            8 threads ListPerThread               30.6     32.1     33.1      1.54X      1.59X       1.6X
                   1 threads NoList               20.6     21.8     22.3      1.04X      1.08X      1.08X
                   2 threads NoList               29.3     30.6       32      1.47X      1.51X      1.54X
                   4 threads NoList               22.2     23.1     23.8      1.11X      1.14X      1.15X
                   8 threads NoList                 14     14.3       15     0.704X      0.71X     0.722X

FreeLists 0 iters on 256 kb:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
                                                                         (relative) (relative) (relative)
---------------------------------------------------------------------------------------------------------
               1 threads SharedList               15.5     16.1     16.4         1X         1X         1X
               2 threads SharedList               8.46     8.92     9.46     0.545X     0.555X     0.578X
               4 threads SharedList               7.31     7.48     7.69     0.471X     0.465X      0.47X
               8 threads SharedList               5.91     6.04     6.21     0.381X     0.376X     0.379X
            1 threads ListPerThread               15.5       16     16.5     0.999X     0.995X      1.01X
            2 threads ListPerThread               24.9       26     27.1       1.6X      1.62X      1.66X
            4 threads ListPerThread               24.8     26.9     29.6       1.6X      1.67X      1.81X
            8 threads ListPerThread               18.4     21.9     23.1      1.18X      1.36X      1.41X
                   1 threads NoList               14.6     15.4     15.8     0.939X     0.956X     0.967X
                   2 threads NoList               17.5     18.8     19.2      1.12X      1.17X      1.17X
                   4 threads NoList               11.7     12.2     12.6     0.756X     0.757X     0.773X
                   8 threads NoList               6.42     6.61     6.82     0.414X     0.411X     0.417X

FreeLists 1 iters on 64 kb:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
                                                                         (relative) (relative) (relative)
---------------------------------------------------------------------------------------------------------
               1 threads SharedList               1.38     1.39      1.4         1X         1X         1X
               2 threads SharedList               1.77     1.81     1.84      1.28X       1.3X      1.31X
               4 threads SharedList               1.86     1.96     2.03      1.35X      1.41X      1.45X
               8 threads SharedList               1.55     1.68     1.77      1.13X      1.21X      1.26X
            1 threads ListPerThread               1.38     1.39      1.4         1X         1X         1X
            2 threads ListPerThread               2.05     2.13     2.19      1.49X      1.53X      1.56X
            4 threads ListPerThread               2.07      2.2     2.32      1.51X      1.59X      1.66X
            8 threads ListPerThread               1.52     1.63     1.71      1.11X      1.17X      1.22X
                   1 threads NoList               1.04     1.05     1.05     0.754X     0.754X     0.748X
                   2 threads NoList               1.57     1.58     1.61      1.14X      1.14X      1.15X
                   4 threads NoList                1.9     2.09     2.17      1.38X       1.5X      1.55X
                   8 threads NoList               1.49      1.6     1.73      1.08X      1.16X      1.24X

FreeLists 1 iters on 128 kb:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
                                                                         (relative) (relative) (relative)
---------------------------------------------------------------------------------------------------------
               1 threads SharedList              0.557    0.561    0.561         1X         1X         1X
               2 threads SharedList               0.75    0.765    0.772      1.35X      1.36X      1.38X
               4 threads SharedList              0.759    0.793    0.833      1.36X      1.41X      1.48X
               8 threads SharedList              0.661    0.685    0.714      1.19X      1.22X      1.27X
            1 threads ListPerThread              0.557    0.561    0.561         1X         1X         1X
            2 threads ListPerThread              0.817    0.833     0.85      1.47X      1.48X      1.51X
            4 threads ListPerThread              0.802    0.842    0.885      1.44X       1.5X      1.58X
            8 threads ListPerThread              0.621    0.667    0.697      1.12X      1.19X      1.24X
                   1 threads NoList                0.4    0.404    0.404     0.719X     0.719X     0.719X
                   2 threads NoList              0.596    0.607    0.621      1.07X      1.08X      1.11X
                   4 threads NoList              0.724    0.773     0.81       1.3X      1.38X      1.44X
                   8 threads NoList              0.561    0.619     0.65      1.01X       1.1X      1.16X

FreeLists 1 iters on 256 kb:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
                                                                         (relative) (relative) (relative)
---------------------------------------------------------------------------------------------------------
               1 threads SharedList               0.25    0.252    0.255         1X         1X         1X
               2 threads SharedList              0.355    0.365    0.369      1.42X      1.45X      1.45X
               4 threads SharedList              0.297    0.306    0.318      1.19X      1.21X      1.25X
               8 threads SharedList              0.315    0.324    0.337      1.26X      1.28X      1.32X
            1 threads ListPerThread              0.252    0.255    0.255      1.01X      1.01X         1X
            2 threads ListPerThread              0.355    0.358    0.381      1.42X      1.42X      1.49X
            4 threads ListPerThread              0.296    0.309    0.345      1.18X      1.22X      1.35X
            8 threads ListPerThread              0.291    0.297    0.304      1.17X      1.18X      1.19X
                   1 threads NoList              0.143    0.143    0.143     0.571X     0.566X      0.56X
                   2 threads NoList              0.224    0.226    0.229     0.897X     0.897X     0.897X
                   4 threads NoList              0.275    0.283    0.294       1.1X      1.12X      1.15X
                   8 threads NoList              0.278    0.288    0.297      1.11X      1.14X      1.17X

FreeLists 2 iters on 64 kb:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
                                                                         (relative) (relative) (relative)
---------------------------------------------------------------------------------------------------------
               1 threads SharedList              0.772    0.779    0.779         1X         1X         1X
               2 threads SharedList               1.17     1.18     1.19      1.51X      1.51X      1.53X
               4 threads SharedList               1.36      1.5     1.65      1.77X      1.92X      2.12X
               8 threads SharedList                1.4     1.46     1.51      1.81X      1.87X      1.94X
            1 threads ListPerThread              0.772    0.779    0.779         1X         1X         1X
            2 threads ListPerThread               1.27     1.29      1.3      1.65X      1.65X      1.67X
            4 threads ListPerThread                1.6     1.73     1.87      2.08X      2.22X       2.4X
            8 threads ListPerThread               1.48     1.54     1.59      1.92X      1.98X      2.04X
                   1 threads NoList              0.644     0.65     0.65     0.834X     0.835X     0.835X
                   2 threads NoList               1.09      1.1     1.11      1.41X      1.41X      1.43X
                   4 threads NoList               1.45     1.63     1.82      1.88X       2.1X      2.34X
                   8 threads NoList               1.43      1.5     1.54      1.85X      1.92X      1.97X

FreeLists 2 iters on 128 kb:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
                                                                         (relative) (relative) (relative)
---------------------------------------------------------------------------------------------------------
               1 threads SharedList              0.365    0.365    0.369         1X         1X         1X
               2 threads SharedList              0.587    0.593    0.604      1.61X      1.62X      1.64X
               4 threads SharedList              0.636    0.667    0.701      1.74X      1.82X       1.9X
               8 threads SharedList              0.582    0.644    0.655      1.59X      1.76X      1.78X
            1 threads ListPerThread              0.365    0.369    0.369         1X      1.01X         1X
            2 threads ListPerThread              0.586    0.591    0.621       1.6X      1.62X      1.68X
            4 threads ListPerThread               0.64    0.714    0.765      1.75X      1.95X      2.07X
            8 threads ListPerThread              0.571    0.613     0.65      1.56X      1.68X      1.76X
                   1 threads NoList              0.294    0.297    0.297     0.805X     0.813X     0.805X
                   2 threads NoList              0.455     0.46    0.468      1.25X      1.26X      1.27X
                   4 threads NoList               0.55     0.62     0.67      1.51X       1.7X      1.82X
                   8 threads NoList              0.519    0.539    0.566      1.42X      1.48X      1.54X

FreeLists 2 iters on 256 kb:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
                                                                         (relative) (relative) (relative)
---------------------------------------------------------------------------------------------------------
               1 threads SharedList               0.15     0.15    0.151         1X         1X         1X
               2 threads SharedList              0.226    0.233    0.235      1.51X      1.56X      1.56X
               4 threads SharedList              0.263    0.275    0.286      1.76X      1.84X      1.89X
               8 threads SharedList               0.26    0.288    0.306      1.74X      1.93X      2.03X
            1 threads ListPerThread               0.15     0.15    0.151         1X         1X         1X
            2 threads ListPerThread              0.245    0.248     0.27      1.64X      1.66X      1.79X
            4 threads ListPerThread              0.238    0.263    0.278      1.59X      1.76X      1.84X
            8 threads ListPerThread               0.22    0.231     0.24      1.47X      1.54X      1.59X
                   1 threads NoList              0.115    0.117    0.117     0.772X     0.779X     0.772X
                   2 threads NoList              0.182    0.185    0.187      1.22X      1.24X      1.24X
                   4 threads NoList              0.226    0.243    0.255      1.51X      1.63X      1.69X
                   8 threads NoList              0.238     0.25    0.262      1.59X      1.67X      1.73X

FreeLists 4 iters on 64 kb:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
                                                                         (relative) (relative) (relative)
---------------------------------------------------------------------------------------------------------
               1 threads SharedList              0.414    0.414    0.418         1X         1X         1X
               2 threads SharedList              0.714    0.721    0.728      1.72X      1.74X      1.74X
               4 threads SharedList              0.883    0.965      1.1      2.13X      2.33X      2.63X
               8 threads SharedList               1.09     1.11     1.14      2.62X      2.68X      2.73X
            1 threads ListPerThread              0.411    0.414    0.418     0.991X         1X         1X
            2 threads ListPerThread               0.75    0.759    0.765      1.81X      1.83X      1.83X
            4 threads ListPerThread              0.953     1.08     1.25       2.3X      2.61X      2.99X
            8 threads ListPerThread               1.03     1.09     1.12      2.48X      2.62X      2.68X
                   1 threads NoList              0.381    0.385    0.426     0.919X     0.928X      1.02X
                   2 threads NoList              0.679    0.685    0.694      1.64X      1.65X      1.66X
                   4 threads NoList              0.924     1.05     1.18      2.23X      2.52X      2.81X
                   8 threads NoList               1.06     1.11     1.15      2.55X      2.68X      2.75X

FreeLists 4 iters on 128 kb:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
                                                                         (relative) (relative) (relative)
---------------------------------------------------------------------------------------------------------
               1 threads SharedList              0.214    0.216    0.216         1X         1X         1X
               2 threads SharedList              0.352    0.355    0.355      1.65X      1.65X      1.65X
               4 threads SharedList              0.362     0.46    0.519      1.69X      2.13X      2.41X
               8 threads SharedList              0.442    0.464    0.481      2.07X      2.15X      2.23X
            1 threads ListPerThread              0.214    0.216    0.216         1X         1X         1X
            2 threads ListPerThread              0.352    0.355    0.358      1.65X      1.65X      1.66X
            4 threads ListPerThread              0.376      0.5    0.545      1.76X      2.32X      2.52X
            8 threads ListPerThread                0.4    0.426    0.447      1.87X      1.97X      2.07X
                   1 threads NoList              0.165    0.167    0.167     0.773X     0.773X     0.773X
                   2 threads NoList              0.301    0.304    0.306      1.41X      1.41X      1.42X
                   4 threads NoList              0.345    0.434    0.482      1.62X      2.01X      2.24X
                   8 threads NoList              0.404    0.434    0.451      1.89X      2.01X      2.09X

FreeLists 4 iters on 256 kb:Function  iters/ms   10%ile   50%ile   90%ile     10%ile     50%ile     90%ile
                                                                         (relative) (relative) (relative)
---------------------------------------------------------------------------------------------------------
               1 threads SharedList              0.102    0.103    0.103         1X         1X         1X
               2 threads SharedList              0.162    0.167    0.168      1.59X      1.62X      1.64X
               4 threads SharedList              0.202    0.222    0.238      1.98X      2.16X      2.31X
               8 threads SharedList              0.204    0.222     0.24         2X      2.16X      2.33X
            1 threads ListPerThread              0.102    0.102    0.103         1X     0.991X         1X
            2 threads ListPerThread              0.163    0.167    0.167       1.6X      1.62X      1.62X
            4 threads ListPerThread              0.188    0.204     0.22      1.85X      1.98X      2.14X
            8 threads ListPerThread              0.187    0.198      0.2      1.84X      1.93X      1.95X
                   1 threads NoList             0.0833   0.0833   0.0833     0.818X     0.811X     0.811X
                   2 threads NoList              0.137     0.14     0.14      1.35X      1.36X      1.36X
                   4 threads NoList              0.176     0.19    0.206      1.73X      1.85X         2X
                   8 threads NoList              0.176    0.208    0.218      1.73X      2.02X      2.12X
*/
enum class FreeListMode {
  SHARED_LIST, // One list shared between all threads.
  LIST_PER_THREAD, // One list per thread.
  NO_LIST, // No list - just allocate and free directly.
};

struct BenchmarkParams {
  /// Number of concurrent threads.
  int num_threads;

  /// How to set up the free lists.
  FreeListMode free_list_mode;

  /// Number of iterations of work to do between freelist operations.
  int work_iterations;

  /// Size of allocation to make.
  int64_t allocation_size;
};

/// Ensure that locks don't share cache lines.
struct LockedList : public CacheLineAligned {
  SpinLock lock;
  FreeList list;
};

static const char* ModeString(FreeListMode mode) {
  switch (mode) {
    case FreeListMode::SHARED_LIST:
      return "SharedList";
    case FreeListMode::LIST_PER_THREAD:
      return "ListPerThread";
    case FreeListMode::NO_LIST:
      return "NoList";
    default:
      return "Invalid";
  }
}

static const int NUM_OPERATIONS_PER_BATCH = 512;
// Choose a list size large enough so that most allocs should hit in the free list
// but small enough that it will sometimes fill up.
static const int MAX_LIST_ENTRIES = 64;

static const int ALLOC_OP = 0;
static const int FREE_OP = 1;

// Not a static allocation because SystemAllocator's constructor uses gperftool's
// MallocExtension instance, which may not yet be defined due to constructor order
static SystemAllocator* allocator = nullptr;

// Simulate doing some work with the buffer.
void DoWork(uint8_t* data, int64_t len) {
  // Touch all the data in the allocation. This is about the minimum amount of
  // work we're likely to do with a buffer in a real workload.
  memset(data, 1, len);
}

/// Make an allocation and do some work on it.
void DoAlloc(const BenchmarkParams& params, LockedList* free_list,
    vector<BufferPool::BufferHandle>* buffers) {
  BufferPool::BufferHandle buffer;
  bool got_buffer = false;
  if (free_list != nullptr) {
    lock_guard<SpinLock> l(free_list->lock);
    got_buffer = free_list->list.PopFreeBuffer(&buffer);
  }
  if (!got_buffer) {
    Status status = allocator->Allocate(params.allocation_size, &buffer);
    if (!status.ok()) LOG(FATAL) << "Failed alloc " << status.msg().msg();
  }
  // Do some processing to simulate a vaguely realistic work pattern.
  for (int j = 0; j < params.work_iterations; ++j) {
    DoWork(buffer.data(), buffer.len());
  }
  buffers->emplace_back(move(buffer));
}

/// Free an allocation.
void DoFree(const BenchmarkParams& params, LockedList* free_list,
    vector<BufferPool::BufferHandle>* buffers) {
  if (!buffers->empty()) {
    if (free_list != nullptr) {
      lock_guard<SpinLock> l(free_list->lock);
      FreeList* list = &free_list->list;
      list->AddFreeBuffer(move(buffers->back()));
      if (list->Size() > MAX_LIST_ENTRIES) {
        // Discard around 1/4 of the buffers to amortise the cost of sorting.
        vector<BufferHandle> buffers =
            list->GetBuffersToFree(list->Size() - MAX_LIST_ENTRIES * 3 / 4);
        for (BufferHandle& buffer : buffers) allocator->Free(move(buffer));
      }
    } else {
      allocator->Free(move(buffers->back()));
    }
    buffers->pop_back();
  }
}

/// Execute 'num_operations' on 'free_list' according to the parameters in
/// 'params'.
void FreeListBenchmarkThread(int thread_id, int num_operations,
    const BenchmarkParams& params, LockedList* free_list) {
  mt19937_64 rng(thread_id);
  vector<BufferPool::BufferHandle> buffers;
  int ops_done = 0;
  while (ops_done < num_operations) {
    // Execute a number of the same operation in a row.
    const int64_t op = uniform_int_distribution<int64_t>(0, 1)(rng);
    const int num_ops = uniform_int_distribution<int64_t>(1, 10)(rng);
    for (int i = 0; i < num_ops; ++i) {
      if (op == ALLOC_OP) {
        DoAlloc(params, free_list, &buffers);
      } else {
        DCHECK_EQ(op, FREE_OP);
        DoFree(params, free_list, &buffers);
      }
    }
    ops_done += num_ops;
  }

  for (BufferHandle& buffer : buffers) allocator->Free(move(buffer));
}

/// Execute the benchmark with the BenchmarkParams passed via 'data'.
void FreeListBenchmark(int batch_size, void* data) {
  ObjectPool pool;
  const BenchmarkParams& params = *static_cast<BenchmarkParams*>(data);
  thread_group threads;
  vector<LockedList*> free_lists;
  if (params.free_list_mode == FreeListMode::SHARED_LIST) {
    free_lists.push_back(pool.Add(new LockedList));
  }

  for (int i = 0; i < params.num_threads; ++i) {
    if (params.free_list_mode == FreeListMode::LIST_PER_THREAD) {
      free_lists.push_back(pool.Add(new LockedList));
    }
    // Divide operations between threads.
    int ops_per_thread = batch_size * NUM_OPERATIONS_PER_BATCH / params.num_threads;
    auto* free_list = free_lists.empty() ? nullptr : free_lists.back();
    threads.add_thread(new thread(FreeListBenchmarkThread, i,
        ops_per_thread, params, free_list));
  }
  threads.join_all();

  // Empty out all of the free lists.
  for (LockedList* free_list : free_lists) {
    vector<BufferHandle> buffers =
        free_list->list.GetBuffersToFree(free_list->list.Size());
    for (BufferHandle& buffer : buffers) allocator->Free(move(buffer));
  }
}

int main(int argc, char** argv) {
  CpuInfo::Init();
  cout << endl << Benchmark::GetMachineInfo() << endl << endl;

  allocator = new SystemAllocator(64 * 1024);
  ObjectPool pool;

  for (int work_iterations : {0, 1, 2, 4}) {
    // Don't test allocations beyond 256KB - by that point we are purely memory-bound.
    for (int allocation_size_kb : {64, 128, 256}) {
      Benchmark suite(Substitute("FreeLists $0 iters on $1 kb",
            work_iterations, allocation_size_kb));
      for (FreeListMode free_list_mode : {FreeListMode::SHARED_LIST,
               FreeListMode::LIST_PER_THREAD, FreeListMode::NO_LIST}) {
        // Vary concurrency up to the # of logical cores.
        for (int num_threads : {1, 2, 4, CpuInfo::num_cores()}) {
          BenchmarkParams* params = pool.Add(new BenchmarkParams());
          params->num_threads = num_threads;
          params->free_list_mode = free_list_mode;
          params->work_iterations = work_iterations;
          params->allocation_size = allocation_size_kb * 1024;
          suite.AddBenchmark(
              Substitute("$0 threads $1", num_threads, ModeString(free_list_mode)),
              FreeListBenchmark, params);
        }
      }
      // Increase benchmark time to get more accurate result.
      cout << suite.Measure(100) << endl;
    }
  }
}
