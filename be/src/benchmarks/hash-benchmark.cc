// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include <iostream>
#include <stdlib.h>
#include <stdio.h>
#include <vector>

#include <boost/functional/hash.hpp>

#include "experiments/data-provider.h"
#include "runtime/raw-value.h"
#include "runtime/string-value.h"
#include "util/benchmark.h"
#include "util/hash-util.h"

using namespace boost;
using namespace impala;
using namespace std;

// Benchmark tests for hashing tuples.  There are two sets of inputs
// that are benchmarked.  The 'Int' set which consists of hashing tuples
// with 4 int32_t values.  The 'Mixed' set consists of tuples with different
// data types, including strings.  The resulting hashes are put into 1000 
// buckets so the expected number of collisions is roughly ~1/3.
// 
// n is the number of buckets, k is the number of items
// Expected(collisions) = n - k + E(X) 
//                      = n - k + k(1 - 1/k)^n
// For k == n (1000 items into 1000 buckets)
//                      = lim n->inf n(1 - 1/n) ^n
//                      = n / e 
//                      = 367
// Results:
//   FVN Int Rate: 95.9596
//   Boost Int Rate: 178.324
//   Crc Int Rate: 403.099
//   
//   FVN Int Collisions: 395
//   Boost Int Collisions: 388
//   Crc Int Collisions: 377
//   
//   FVN Mixed Rate: 83.271
//   Boost Mixed Rate: 73.743
//   Crc Mixed Rate: 297.051
//   
//   FVN Mixed Collisions: 386
//   Boost Mixed Collisions: 379
//   Crc Mixed Collisions: 357

struct TestData {
  void* data;
  int num_cols;
  int num_rows;
  vector<int32_t> results;
};

void TestFvnIntHash(int batch, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  int rows = data->num_rows;
  int cols = data->num_cols;
  for (int i = 0; i < batch; ++i) {
    int32_t* values = reinterpret_cast<int32_t*>(data->data);
    for (int j = 0; j < rows; ++j) {
      size_t hash = HashUtil::FVN_SEED;
      for (int k = 0; k < cols; ++k) {
        hash = HashUtil::FvnHash(&values[k], sizeof(uint32_t), hash);
      }
      data->results[j] = hash;
      values += cols;
    }
  }
}

void TestCrcIntHash(int batch, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  int rows = data->num_rows;
  int cols = data->num_cols;
  for (int i = 0; i < batch; ++i) {
    int32_t* values = reinterpret_cast<int32_t*>(data->data);
    for (int j = 0; j < rows; ++j) {
      size_t hash = HashUtil::FVN_SEED;
      for (int k = 0; k < cols; ++k) {
        hash = HashUtil::CrcHash(&values[k], sizeof(uint32_t), hash);
      }
      data->results[j] = hash;
      values += cols;
    }
  }
}

void TestBoostIntHash(int batch, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  int rows = data->num_rows;
  int cols = data->num_cols;
  for (int i = 0; i < batch; ++i) {
    int32_t* values = reinterpret_cast<int32_t*>(data->data);
    for (int j = 0; j < rows; ++j) {
      size_t h = HashUtil::FVN_SEED;
      for (int k = 0; k < cols; ++k) {
        size_t hash_value = hash<int32_t>().operator()(values[k]);
        hash_combine(h, hash_value);
      }
      data->results[j] = h;
      values += cols;
    }
  }
}

void TestFvnMixedHash(int batch, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  int rows = data->num_rows;
  for (int i = 0; i < batch; ++i) {
    char* values = reinterpret_cast<char*>(data->data);
    for (int j = 0; j < rows; ++j) {
      size_t hash = HashUtil::FVN_SEED;
      
      hash = HashUtil::FvnHash(values, sizeof(int8_t), hash);
      values += sizeof(int8_t);
      
      hash = HashUtil::FvnHash(values, sizeof(int32_t), hash);
      values += sizeof(int32_t);
      
      StringValue* str = reinterpret_cast<StringValue*>(values);
      hash = HashUtil::FvnHash(str->ptr, str->len, hash);
      values += sizeof(StringValue);

      hash = HashUtil::FvnHash(values, sizeof(int64_t), hash);
      values += sizeof(int64_t);

      data->results[j] = hash;
    }
  }
}

void TestCrcMixedHash(int batch, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  int rows = data->num_rows;
  for (int i = 0; i < batch; ++i) {
    char* values = reinterpret_cast<char*>(data->data);
    for (int j = 0; j < rows; ++j) {
      size_t hash = HashUtil::FVN_SEED;
      
      hash = HashUtil::CrcHash(values, sizeof(int8_t), hash);
      values += sizeof(int8_t);
      
      hash = HashUtil::CrcHash(values, sizeof(int32_t), hash);
      values += sizeof(int32_t);
      
      StringValue* str = reinterpret_cast<StringValue*>(values);
      hash = HashUtil::CrcHash(str->ptr, str->len, hash);
      values += sizeof(StringValue);

      hash = HashUtil::CrcHash(values, sizeof(int64_t), hash);
      values += sizeof(int64_t);

      data->results[j] = hash;
    }
  }
}


void TestBoostMixedHash(int batch, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  int rows = data->num_rows;
  for (int i = 0; i < batch; ++i) {
    char* values = reinterpret_cast<char*>(data->data);
    for (int j = 0; j < rows; ++j) {
      size_t h = HashUtil::FVN_SEED;
      
      size_t hash_value = hash<int8_t>().operator()(*reinterpret_cast<int8_t*>(values));
      hash_combine(h, hash_value);
      values += sizeof(int8_t);

      hash_value = hash<int32_t>().operator()(*reinterpret_cast<int32_t*>(values));
      hash_combine(h, hash_value);
      values += sizeof(int32_t);
      
      StringValue* str = reinterpret_cast<StringValue*>(values);
      hash_value = hash_range<char*>(str->ptr, str->ptr + str->len);
      hash_combine(h, hash_value);
      values += sizeof(StringValue);
      
      hash_value = hash<int64_t>().operator()(*reinterpret_cast<int64_t*>(values));
      hash_combine(h, hash_value);
      values += sizeof(int64_t);
      
      data->results[j] = h;
    }
  }
}

int NumCollisions(TestData* data, int num_buckets) {
  vector<bool> buckets;
  buckets.resize(num_buckets);

  int num_collisions = 0;
  for (int i = 0; i < data->results.size(); ++i) {
    uint32_t hash = data->results[i];
    int bucket = hash % num_buckets;
    if (buckets[bucket]) ++num_collisions;
    buckets[bucket] = true;
  }
  return num_collisions;
}

int main(int argc, char **argv) {

  const int NUM_ROWS = 1024;

  ObjectPool obj_pool;
  MemPool mem_pool;
  RuntimeProfile profile(&obj_pool, "DataGen");
  DataProvider data_provider(&mem_pool, &profile);

  // Test a tuple consisting of just 4 int cols.  This is representative of
  // group by queries.  The test is hardcoded to know it will only be int cols
  vector<DataProvider::ColDesc> int_cols;
  int_cols.push_back(DataProvider::ColDesc::Create<int32_t>(0, 10000));
  int_cols.push_back(DataProvider::ColDesc::Create<int32_t>(0, 1000000));
  int_cols.push_back(DataProvider::ColDesc::Create<int32_t>(0, 1000000));
  int_cols.push_back(DataProvider::ColDesc::Create<int32_t>(0, 1000000));

  data_provider.Reset(NUM_ROWS, NUM_ROWS, int_cols);

  TestData int_data;
  int_data.data = data_provider.NextBatch(&int_data.num_rows);
  int_data.num_cols = int_cols.size();
  int_data.results.resize(int_data.num_rows);
  
  // Some mixed col types.  The test hash function will know the exact
  // layout.  This is reasonable to do since we can use llvm
  string min_std_str("aaa");
  string max_std_str("zzzzzzzzzz");

  StringValue min_str(const_cast<char*>(min_std_str.c_str()), min_std_str.size());
  StringValue max_str(const_cast<char*>(max_std_str.c_str()), max_std_str.size());

  vector<DataProvider::ColDesc> mixed_cols;
  mixed_cols.push_back(DataProvider::ColDesc::Create<int8_t>(0, 25));
  mixed_cols.push_back(DataProvider::ColDesc::Create<int32_t>(0, 10000));
  mixed_cols.push_back(DataProvider::ColDesc::Create<StringValue>(min_str, max_str));
  mixed_cols.push_back(DataProvider::ColDesc::Create<int64_t>(0, 100000000));
  data_provider.Reset(NUM_ROWS, NUM_ROWS, mixed_cols);

  TestData mixed_data;
  mixed_data.data = data_provider.NextBatch(&mixed_data.num_rows);
  mixed_data.num_cols = mixed_cols.size();
  mixed_data.results.resize(mixed_data.num_rows);

  TestFvnIntHash(10, &int_data);  // warm-up run
  double fvn_int_rate = Benchmark::Measure(TestFvnIntHash, &int_data);
  int fvn_int_collisions = NumCollisions(&int_data, NUM_ROWS);
  
  double boost_int_rate = Benchmark::Measure(TestBoostIntHash, &int_data);
  int boost_int_collisions = NumCollisions(&int_data, NUM_ROWS);
  
  double crc_int_rate = Benchmark::Measure(TestCrcIntHash, &int_data);
  int crc_int_collisions = NumCollisions(&int_data, NUM_ROWS);
  
  TestFvnMixedHash(10, &mixed_data);  // warm-up run
  double fvn_mixed_rate = Benchmark::Measure(TestFvnMixedHash, &mixed_data);
  int fvn_mixed_collisions = NumCollisions(&mixed_data, NUM_ROWS);
  
  double boost_mixed_rate = Benchmark::Measure(TestBoostMixedHash, &mixed_data);
  int boost_mixed_collisions = NumCollisions(&mixed_data, NUM_ROWS);
  
  double crc_mixed_rate = Benchmark::Measure(TestCrcMixedHash, &mixed_data);
  int crc_mixed_collisions = NumCollisions(&mixed_data, NUM_ROWS);

  cout << "FVN Int Rate: " << fvn_int_rate << endl;
  cout << "Boost Int Rate: " << boost_int_rate << endl;
  cout << "Crc Int Rate: " << crc_int_rate << endl;
  cout << endl;
  
  cout << "FVN Int Collisions: " << fvn_int_collisions << endl;
  cout << "Boost Int Collisions: " << boost_int_collisions << endl;
  cout << "Crc Int Collisions: " << crc_int_collisions << endl;
  cout << endl;

  cout << "FVN Mixed Rate: " << fvn_mixed_rate << endl;
  cout << "Boost Mixed Rate: " << boost_mixed_rate << endl;
  cout << "Crc Mixed Rate: " << crc_mixed_rate << endl;
  cout << endl;
  
  cout << "FVN Mixed Collisions: " << fvn_mixed_collisions << endl;
  cout << "Boost Mixed Collisions: " << boost_mixed_collisions << endl;
  cout << "Crc Mixed Collisions: " << crc_mixed_collisions << endl;

  return 0;
}

