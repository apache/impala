// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

#include <stdlib.h>
#include <stdio.h>
#include <iostream>

#include "experiments/data-provider.h"
#include "util/cpu-info.h"
#include "util/perf-counters.h"
#include "runtime/mem-pool.h"
#include "runtime/string-value.h"

using namespace boost;
using namespace std;
using namespace impala;

// This test will generate 20 rows of data, in all the types supported by the 
// data provider.
int main(int argc, char **argv) {

  string min_std_str("a");
  string max_std_str("zzzzzz");

  StringValue min_str(const_cast<char*>(min_std_str.c_str()), min_std_str.size());
  StringValue max_str(const_cast<char*>(max_std_str.c_str()), max_std_str.size());

  vector<DataProvider::ColDesc> cols;
  cols.push_back(DataProvider::ColDesc::Create<bool>(false, true));
  cols.push_back(DataProvider::ColDesc::Create<bool>(false, true, DataProvider::SEQUENTIAL));
  cols.push_back(DataProvider::ColDesc::Create<int8_t>(0, 8));
  cols.push_back(DataProvider::ColDesc::Create<int8_t>(0, 8, DataProvider::SEQUENTIAL));
  cols.push_back(DataProvider::ColDesc::Create<int16_t>(8, 16));
  cols.push_back(DataProvider::ColDesc::Create<int32_t>(16, 32));
  cols.push_back(DataProvider::ColDesc::Create<int64_t>(32, 64));
  cols.push_back(DataProvider::ColDesc::Create<float>(-1, 1));
  cols.push_back(DataProvider::ColDesc::Create<float>(0, 5, DataProvider::SEQUENTIAL));
  cols.push_back(DataProvider::ColDesc::Create<double>(200, 300));
  cols.push_back(DataProvider::ColDesc::Create<StringValue>(min_str, max_str));

  ObjectPool obj_pool;
  RuntimeProfile profile(&obj_pool, "DataGenTest");
  
  MemPool pool;
  DataProvider provider(&pool, &profile);
  provider.Reset(20, 2, cols);
  int rows;
  void* data;

  cout << "Row size: " << provider.row_size() << endl;

  while ( (data = provider.NextBatch(&rows)) != NULL) {
    provider.Print(&cout, reinterpret_cast<char*>(data), rows);
  }

  profile.PrettyPrint(&cout);

  cout << endl << "Done." << endl;
  return 0;
}

