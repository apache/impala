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


#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <vector>

#include "common/compiler-util.h"
#include "common/object-pool.h"
#include "experiments/data-provider.h"
#include "runtime/mem-pool.h"
#include "runtime/mem-tracker.h"
#include "runtime/string-value.h"
#include "util/cpu-info.h"
#include "util/runtime-profile-counters.h"

#define PRINT_RESULT 0

#define UNUSED_BITS 16
#define USED_BITS 48
#define POINTER_LOWER_BITS_MASK 0x0000FFFFFFFFFFFF

#include "common/names.h"

using namespace impala;

inline void Memcpy16(uint8_t* dst, uint8_t* src) {
  reinterpret_cast<uint64_t*>(dst)[0] = reinterpret_cast<uint64_t*>(src)[0];
  reinterpret_cast<uint64_t*>(dst)[1] = reinterpret_cast<uint64_t*>(src)[1];
}

struct PointerValue {
  union {
    uint64_t val;
    uint8_t* ptr;
  };

  template<typename T>
  T GetPointer() {
    return reinterpret_cast<T>(val & POINTER_LOWER_BITS_MASK);
  }

  uint16_t GetValue() const {
    return static_cast<uint16_t>(val >> USED_BITS);
  }

  template<typename T>
  void Get(T& t, uint16_t& v) {
    t = reinterpret_cast<T>(val & POINTER_LOWER_BITS_MASK);
    v = static_cast<uint16_t>(val >> USED_BITS);
  }

  void UpdatePointer(void* p) {
    val = val >> USED_BITS;
    val = val << USED_BITS;
    val = val | reinterpret_cast<uint64_t>(p);
  }

  void UpdateValue(uint16_t v) {
    val = val << UNUSED_BITS;
    val = val >> UNUSED_BITS;
    val = val | (static_cast<uint64_t>(v) << USED_BITS);
  }

  void Update(void* p, int16_t v) {
    val = reinterpret_cast<uint64_t>(p);
    val = val | (static_cast<uint64_t>(v) << USED_BITS);
  }
};

template <class T>
T NextPowerOfTwo(T k) {
  k--;
  for (int i = 1; i < sizeof(T) * CHAR_BIT; i <<=1) k = k | k >> i;
  return k+1;
}

class DataPartitioner {
 public:
  DataPartitioner(MemPool* pool, RuntimeProfile* profile, int size, int hash_offset) {
    pool_ = pool;
    profile_ = profile;
    size_ = size;
    hash_offset_ = hash_offset;

    bytes_allocated_ = ADD_COUNTER(profile, "BytesAllocated", TUnit::BYTES);
    bytes_copied_ = ADD_COUNTER(profile, "BytesCopied", TUnit::BYTES);
    add_time_ = ADD_COUNTER(profile, "AddTime", TUnit::CPU_TICKS);
    split_time_ = ADD_COUNTER(profile, "SplitTime", TUnit::CPU_TICKS);
    splits_ = ADD_COUNTER(profile, "Splits", TUnit::UNIT);

    // Max of tuple size and cache line
    int min_size_per_partition = max(size, 64);
    partitions_per_level_ = NextPowerOfTwo(L1_size / min_size_per_partition) >> 1;
    tuples_per_partition_ = L1_size / size;

    printf("Tuple Size: %d\n", size_);
    printf("Partitions: %d  Per Partition: %d\n", partitions_per_level_, tuples_per_partition_);

    partition_idx_mask_ = partitions_per_level_ - 1;
    build_partitions_.resize(partitions_per_level_);
    child_partitions_.resize(partitions_per_level_);

    for (int i = 0; i < partitions_per_level_; ++i) {
      build_partitions_[i].level = 0;
    }

    partitions_at_level_.resize(6);
    partitions_at_level_[0] = partitions_per_level_;
    partitions_at_level_[1] = partitions_per_level_;
    partitions_at_level_[2] = partitions_per_level_;
    partitions_at_level_[3] = partitions_per_level_;
    partitions_at_level_[4] = partitions_per_level_;
    partitions_at_level_[5] = partitions_per_level_;

    split_counts_.resize(partitions_per_level_);

    outputs_.resize(partitions_per_level_);
    for (int i = 0; i < outputs_.size(); ++i) {
      outputs_[i].Update(NULL, tuples_per_partition_);
    }
  }

  void AddData(int n, uint8_t* data);

  int size_per_block() const {
    return tuples_per_partition_;
  }

  struct Partition {
    vector<uint8_t*> blocks;
    int num_last_block;

    Partition() {
      num_last_block = 0;
    }

    Partition(const vector<uint8_t*>& blocks, int num) {
      this->blocks = blocks;
      this->num_last_block = num;
    }
  };

  int NumTuples(const Partition& partition) const {
    return partition.num_last_block + (partition.blocks.size() - 1) * tuples_per_partition_;
  }

  int NumTuples(const Partition& partition, int block_idx) const {
    if (block_idx == partition.blocks.size() - 1) return partition.num_last_block;
    return tuples_per_partition_;
  }

  bool Finalize(vector<Partition>* results) {
    SCOPED_TIMER(split_time_);
    bool result = true;
    for (int i = 0; i < build_partitions_.size(); ++i) {
      if (build_partitions_[i].blocks.size() > 0) {
        BuildPartition& build_partition = build_partitions_[i];
        build_partition.num_last_block = NumLastBlock(i);
      }
    }

    for (int i = 0; i < build_partitions_.size(); ++i) {
      if (build_partitions_[i].blocks.size() > 0) {
        BuildPartition& build_partition = build_partitions_[i];
        if (TotalTuples(build_partition) > tuples_per_partition_) {
          if (Split(build_partition)) {
            continue;
          } else {
            result = false;
          }
        }
        results->push_back(ToOutputPartition(build_partition));
      }
    }
    return result;
  }

 private:
  static const int L1_size = 24 * 1024;
  static const int MIN_SPLITS = 4;
  static const int HASH_BIT_SHIFT = 4;
  MemPool* pool_;
  RuntimeProfile* profile_;
  RuntimeProfile::Counter* bytes_allocated_;
  RuntimeProfile::Counter* bytes_copied_;
  RuntimeProfile::Counter* split_time_;
  RuntimeProfile::Counter* add_time_;
  RuntimeProfile::Counter* splits_;

  int size_;
  int hash_offset_;
  int tuples_per_partition_;
  int partitions_per_level_;
  int partition_idx_mask_;
  vector<PointerValue> outputs_;
  vector<int32_t> split_counts_;
  vector<int> partitions_at_level_;

  struct BuildPartition {
    vector<uint8_t*> blocks;
    int num_last_block;
    int level;
  };
  vector<BuildPartition> build_partitions_;
  vector<BuildPartition> child_partitions_;

  uint8_t* Allocate(BuildPartition* partition, int p, int size = 0) {
    if (size == 0) size = tuples_per_partition_ * size_;
    else size = size * size_;
    COUNTER_ADD(bytes_allocated_, size);
    uint8_t* out_mem = pool_->Allocate(size);
    partition->blocks.push_back(out_mem);
    outputs_[p].Update(out_mem, 0);
    return out_mem;
  }

  Partition ToOutputPartition(const BuildPartition& build) const {
    return Partition(build.blocks, build.num_last_block);
  }

  int TotalTuples(const BuildPartition& partition) const {
    return partition.num_last_block + (partition.blocks.size() - 1) * tuples_per_partition_;
  }

  int NumLastBlock(int p) const {
    return outputs_[p].GetValue();
  }

  int NumTuples(const BuildPartition& partition, int block_idx) const {
    if (block_idx == partition.blocks.size() - 1) return partition.num_last_block;
    return tuples_per_partition_;
  }

  bool Split(const BuildPartition& build_partition);
};

void DataPartitioner::AddData(int n, uint8_t* data) {
  SCOPED_TIMER(add_time_);
  COUNTER_ADD(bytes_copied_, n * size_);

  for (int i = 0; i < n; ++i) {
    int32_t hash = *reinterpret_cast<int32_t*>(data + hash_offset_);
    hash &= partition_idx_mask_;
    uint8_t* out_mem = outputs_[hash].GetPointer<uint8_t*>();
    uint16_t size = outputs_[hash].GetValue();
    if (size == tuples_per_partition_) {
      out_mem = Allocate(&build_partitions_[hash], hash);
      size = 0;
    }
    Memcpy16(out_mem, data);
    data += size_;
    outputs_[hash].Update(out_mem + size_, size + 1);
  }
}

bool DataPartitioner::Split(const BuildPartition& build_partition) {
  COUNTER_ADD(splits_, 1);
  const int num_blocks = build_partition.blocks.size();
  const int next_level = build_partition.level + 1;
  if (next_level >= 8) {
    CHECK(false) << "TOO MANY LEVELS: " << next_level;
    return false;
  }
  int partitions = partitions_at_level_[next_level];
  int partition_mask = partitions - 1;

  int new_splits = 0;
  memset(split_counts_.data(), 0, sizeof(split_counts_[0]) * split_counts_.size());

  for (int i = 0; i < num_blocks; ++i) {
    int tuples = NumTuples(build_partition, i);
    uint8_t* hash_slot = build_partition.blocks[i] + hash_offset_;
    for (int j = 0; j < tuples; ++j) {
      int32_t hash = *reinterpret_cast<int32_t*>(hash_slot);
      hash >>= HASH_BIT_SHIFT;
      *reinterpret_cast<int32_t*>(hash_slot) = hash;
      hash &= partition_mask;
      new_splits += !split_counts_[hash];
      split_counts_[hash]++;
      hash_slot += size_;
    }
  }
  if (new_splits < MIN_SPLITS) return false;

  for (int i = 0; i < partitions; ++i) {
    if (split_counts_[i] > 0) {
      COUNTER_ADD(bytes_allocated_, split_counts_[i] * size_);
      uint8_t* mem = pool_->Allocate(split_counts_[i] * size_);
      child_partitions_[i].blocks.clear();
      child_partitions_[i].level = next_level;
      child_partitions_[i].blocks.push_back(mem);
      outputs_[i].ptr = mem;
    }
  }
  COUNTER_ADD(bytes_copied_, TotalTuples(build_partition) * size_);

  for (int i = 0; i < num_blocks; ++i) {
    uint8_t* data = build_partition.blocks[i];
    int tuples = NumTuples(build_partition, i);

    const int offset1 = 0;
    const int offset2 = offset1 + size_;
    const int offset3 = offset2 + size_;
    const int offset4 = offset3 + size_;

    while (tuples >= 4) {
      int32_t hash1 = *reinterpret_cast<int32_t*>(data + hash_offset_ + offset1) & partition_mask;
      int32_t hash2 = *reinterpret_cast<int32_t*>(data + hash_offset_ + offset2) & partition_mask;
      int32_t hash3 = *reinterpret_cast<int32_t*>(data + hash_offset_ + offset3) & partition_mask;
      int32_t hash4 = *reinterpret_cast<int32_t*>(data + hash_offset_ + offset4) & partition_mask;

      uint8_t** out_mem1 = &outputs_[hash1].ptr;
      uint8_t** out_mem2 = &outputs_[hash2].ptr;
      uint8_t** out_mem3 = &outputs_[hash3].ptr;
      uint8_t** out_mem4 = &outputs_[hash4].ptr;

      Memcpy16(*out_mem1, data + offset1);
      *out_mem1 += size_;
      Memcpy16(*out_mem2, data + offset2);
      *out_mem2 += size_;
      Memcpy16(*out_mem3, data + offset3);
      *out_mem3 += size_;
      Memcpy16(*out_mem4, data + offset4);
      *out_mem4 += size_;

      data += size_ * 4;
      tuples -= 4;
    }

    for (int j = 0; j < tuples; ++j) {
      int32_t hash = *reinterpret_cast<int32_t*>(data + hash_offset_) & partition_mask;
      uint8_t* out_mem = outputs_[hash].ptr;
      Memcpy16(out_mem, data);
      data += size_;
      outputs_[hash].ptr = out_mem + size_;
    }
  }

  for (int i = 0; i < partitions; ++i) {
    if (split_counts_[i] > 0) {
      BuildPartition& child = child_partitions_[i];
      child.num_last_block = split_counts_[i];
      build_partitions_.push_back(child);
    }
  }

  return true;
}


int main(int argc, char **argv) {
  google::InitGoogleLogging(argv[0]);

  LOG(ERROR) << "Starting Test";

  vector<DataProvider::ColDesc> cols;
  // Hash Slot
  cols.push_back(DataProvider::ColDesc::Create<int32_t>(0, 0));
  // Grouping Column
  cols.push_back(DataProvider::ColDesc::Create<int64_t>(0, 500000));
  // Aggregate Column
  cols.push_back(DataProvider::ColDesc::Create<float>(-1, 1));

  MemTracker tracker;
  MemPool pool(&tracker);
  ObjectPool obj_pool;
  RuntimeProfile* profile = RuntimeProfile::Create(&obj_pool, "PartitioningTest");

  DataProvider provider(&pool, profile);
  provider.Reset(50*1024*1024, 1024, cols);
  //provider.Reset(100*1024, 1024, cols);
  //provider.Reset(100, 1024, cols);

  DataPartitioner partitioner(&pool, profile, provider.row_size(), 0);

  cout << "Begin processing: " << provider.total_rows() << endl;
  int rows;
  void* data;
  while ( (data = provider.NextBatch(&rows)) != NULL) {
    uint8_t* next_row = reinterpret_cast<uint8_t*>(data);
    // Compute hash values and store them
    for (int i = 0; i < rows; ++i) {
      int32_t* hash = reinterpret_cast<int32_t*>(next_row);
      int64_t* col = reinterpret_cast<int64_t*>(next_row + 4);
      *hash = *col;
      next_row += provider.row_size();
    }
    partitioner.AddData(rows, reinterpret_cast<uint8_t*>(data));
  }
  cout << endl;

  LOG(ERROR) << "Partitioning";

  vector<DataPartitioner::Partition> partitions;
  bool fully_split = partitioner.Finalize(&partitions);

  cout << endl << "After Partitioning" << endl;
  int result_tuples = 0;
  int blocks = 0;
  int largest_partition = 0;
  // Validate results
  for (int i = 0; i < partitions.size(); ++i) {
    DataPartitioner::Partition& partition = partitions[i];
    int tuples = partitioner.NumTuples(partition);
    if (largest_partition < tuples) largest_partition = tuples;
    result_tuples += tuples;
    blocks += partition.blocks.size();
#if PRINT_RESULT
    for (int j = 0; j < partition.blocks.size(); ++j) {
      int rows = partitioner.NumTuples(partition, j);
      provider.Print(&cout, partition.blocks[j], rows);
      cout << "------------------------------" << endl;
    }
#endif
  }
  cout << endl;
  cout << "Fully Split: " << (fully_split ? "yes" : "no") << endl;
  cout << "Partitioned tuples: " << result_tuples << endl;
  cout << "Num Blocks: " << blocks << endl;
  cout << "Num Partitions: " << partitions.size() << endl;
  cout << "Largest Partition: " << largest_partition << endl;;

  cout << endl;
  profile->PrettyPrint(&cout);

  LOG(ERROR) << "Done.";
  return 0;
}
