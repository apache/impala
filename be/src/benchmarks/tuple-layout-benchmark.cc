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
#include <algorithm>
#include "util/benchmark.h"
#include "util/cpu-info.h"
#include "runtime/string-search.h"

#include "common/names.h"

using namespace impala;

// Benchmark tests for padded & aligned vs unpadded tuple layouts.

// Machine Info: Intel(R) Core(TM) i7-2600 CPU @ 3.40GHz
// Tuple Layout:         Function                Rate          Comparison
// ----------------------------------------------------------------------
//               SequentialPadded              0.4013                  1X
//               SequentialImpala              0.4374               1.09X
//            SequentialUnaligned              0.4286              1.068X
//                   RandomPadded              0.1342             0.3345X
//                   RandomImpala              0.1437              0.358X
//                RandomUnaligned              0.1452             0.3619X

#define VALIDATE 0

const int NUM_TUPLES = 1024 * 500;
const int MAX_ID = 10000;

struct UnpaddedTupleStruct {
  int8_t a;
  int8_t b;
  int16_t c;
  float d;
  int64_t id;
  double val;
};

struct PaddedTupleStruct {
  int8_t a;
  double val;
  int16_t c;
  int64_t id;

  static const int UnpaddedSize = 1 + 8 + 2 + 8;
};

struct ImpalaTupleStruct {
  int8_t a;
  int16_t c;
  int64_t id;
  double val;
};

struct TestData {
  double result;
  UnpaddedTupleStruct* unpadded_data;
  PaddedTupleStruct* padded_data;
  ImpalaTupleStruct* impala_data;
  char* unaligned_data;
  vector<int> rand_access_order;
};

void InitTestData(TestData* data) {
  data->unpadded_data =
      (UnpaddedTupleStruct*)malloc(NUM_TUPLES * sizeof(UnpaddedTupleStruct));
  data->padded_data =
      (PaddedTupleStruct*)malloc(NUM_TUPLES * sizeof(PaddedTupleStruct));
  data->impala_data =
      (ImpalaTupleStruct*)malloc(NUM_TUPLES * sizeof(ImpalaTupleStruct));
  data->unaligned_data = (char*)malloc(NUM_TUPLES * PaddedTupleStruct::UnpaddedSize);
  data->rand_access_order.resize(NUM_TUPLES);

  char* unpadded_ptr = data->unaligned_data;
  for (int i = 0; i < NUM_TUPLES; ++i) {
    data->rand_access_order[i] = i;

    int8_t rand_a = rand() % 256;
    double rand_val = rand() / (double)RAND_MAX;
    int16_t rand_c = rand() % 30000;
    int64_t rand_id = rand() % MAX_ID;

    data->padded_data[i].a = rand_a;
    data->padded_data[i].val = rand_val;
    data->padded_data[i].c = rand_c;
    data->padded_data[i].id = rand_id;

    data->unpadded_data[i].id = rand_id;
    data->unpadded_data[i].val = rand_val;

    data->impala_data[i].id = rand_id;
    data->impala_data[i].val = rand_val;

    *reinterpret_cast<int8_t*>(unpadded_ptr) = rand_a;
    unpadded_ptr += 1;
    *reinterpret_cast<double*>(unpadded_ptr) = rand_val;
    unpadded_ptr += 8;
    *reinterpret_cast<int16_t*>(unpadded_ptr) = rand_c;
    unpadded_ptr += 2;
    *reinterpret_cast<int64_t*>(unpadded_ptr) = rand_id;
    unpadded_ptr += 8;
  }

  DCHECK_EQ(unpadded_ptr,
      data->unaligned_data + NUM_TUPLES * PaddedTupleStruct::UnpaddedSize);
  random_shuffle(data->rand_access_order.begin(), data->rand_access_order.end());
}


void TestSequentialUnpadded(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    data->result = 0;
    for (int j = 0; j < NUM_TUPLES; ++j) {
      const UnpaddedTupleStruct& item = data->unpadded_data[j];
      if (item.id > MAX_ID / 2) data->result += item.val;
    }
  }
}

void TestSequentialPadded(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    data->result = 0;
    for (int j = 0; j < NUM_TUPLES; ++j) {
      const PaddedTupleStruct& item = data->padded_data[j];
      if (item.id > MAX_ID / 2) data->result += item.val;
    }
  }
}

void TestSequentialImpala(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    data->result = 0;
    for (int j = 0; j < NUM_TUPLES; ++j) {
      const ImpalaTupleStruct& item = data->impala_data[j];
      if (item.id > MAX_ID / 2) data->result += item.val;
    }
  }
}

void TestSequentialUnaligned(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    data->result = 0;
    char* data_ptr = data->unaligned_data;
    for (int j = 0; j < NUM_TUPLES; ++j) {
      int64_t id = *reinterpret_cast<int64_t*>(data_ptr + 11);
      if (id > MAX_ID / 2) {
        data->result += *reinterpret_cast<double*>(data_ptr + 1);
      }
      data_ptr += PaddedTupleStruct::UnpaddedSize;
    }
  }
}

void TestRandomPadded(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int* order = &data->rand_access_order[0];
    data->result = 0;
    for (int j = 0; j < NUM_TUPLES; ++j) {
      const PaddedTupleStruct& item = data->padded_data[order[j]];
      if (item.id > MAX_ID / 2) data->result += item.val;
    }
  }
}

void TestRandomImpala(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int* order = &data->rand_access_order[0];
    data->result = 0;
    for (int j = 0; j < NUM_TUPLES; ++j) {
      const ImpalaTupleStruct& item = data->impala_data[order[j]];
      if (item.id > MAX_ID / 2) data->result += item.val;
    }
  }
}

void TestRandomUnaligned(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    data->result = 0;
    int* order = &data->rand_access_order[0];
    for (int j = 0; j < NUM_TUPLES; ++j) {
      char* data_ptr = data->unaligned_data + PaddedTupleStruct::UnpaddedSize * order[j];
      int64_t id = *reinterpret_cast<int64_t*>(data_ptr + 11);
      if (id > MAX_ID / 2) {
        data->result += *reinterpret_cast<double*>(data_ptr + 1);
      }
    }
  }
}

int main(int argc, char **argv) {
  CpuInfo::Init();
  cout << Benchmark::GetMachineInfo() << endl;

  DCHECK_EQ(sizeof(UnpaddedTupleStruct), 24);
  DCHECK_EQ(sizeof(PaddedTupleStruct), 32);
  DCHECK_EQ(sizeof(ImpalaTupleStruct), 24);

  TestData data;
  InitTestData(&data);

#if VALIDATE
  TestSequentialUnpadded(1, &data);
  cout << data.result << endl;
  TestSequentialPadded(1, &data);
  cout << data.result << endl;
  TestSequentialImpala(1, &data);
  cout << data.result << endl;
  TestSequentialUnaligned(1, &data);
  cout << data.result << endl;
  TestRandomPadded(1, &data);
  cout << data.result << endl;
  TestRandomImpala(1, &data);
  cout << data.result << endl;
  TestRandomUnaligned(1, &data);
  cout << data.result << endl;
#else
  Benchmark suite("Tuple Layout");
  suite.AddBenchmark("SequentialPadded", TestSequentialPadded, &data);
  suite.AddBenchmark("SequentialImpala", TestSequentialImpala, &data);
  suite.AddBenchmark("SequentialUnaligned", TestSequentialUnaligned, &data);
  suite.AddBenchmark("RandomPadded", TestRandomPadded, &data);
  suite.AddBenchmark("RandomImpala", TestRandomImpala, &data);
  suite.AddBenchmark("RandomUnaligned", TestRandomUnaligned, &data);
  cout << suite.Measure();
#endif

  return 0;
}
