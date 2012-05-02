// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <vector>
#include <sstream>
#include "runtime/string-value.h"
#include "util/benchmark.h"
#include "util/string-parser.h"

using namespace impala;
using namespace std;

#define VALIDATE 1

#if VALIDATE
// Use fabs?
#define VALIDATE_RESULT(actual, expected, str) \
  if (actual != expected) { \
    cout << "Parse Error. " \
         << "String: " << str \
         << ". Parsed: " << actual << endl; \
    exit(-1); \
  }
#else
#define VALIDATE_RESULT(actual, expected, str)
#endif

struct TestData {
  vector<StringValue> data;
  vector<string> memory;
  vector<double> result;
};

void AddTestData(TestData* data, const string& input) {
  data->memory.push_back(input);
  const string& str = data->memory.back();
  data->data.push_back(StringValue(const_cast<char*>(str.c_str()), str.length()));
}

void AddTestData(TestData* data, int n, double min = -10, double max = 10) {
  for (int i = 0; i < n; ++i) {
    double val = rand();
    val /= RAND_MAX;
    val = (val * (max - min)) + min;
    stringstream ss;
    ss << val;
    AddTestData(data, ss.str());
  }
}

void TestAtof(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      data->result[j] = atof(data->data[j].ptr);
    }
  }
}

void TestImpala(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      const StringValue& str = data->data[j];
      StringParser::ParseResult dummy;
      double val = StringParser::StringToFloat<double>(str.ptr, str.len, &dummy);
      VALIDATE_RESULT(val, data->result[j], str.ptr);
      data->result[j] = val;
    }
  }
}

void TestStrtod(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      data->result[j] = strtod(data->data[j].ptr, NULL);
    }
  }
}

int main(int argc, char **argv) {
  TestData data;

  // Most data is probably positive
  AddTestData(&data, 1000, -5, 1000);
  AddTestData(&data, "1.1e12");

  data.result.resize(data.data.size());

  // Run a warmup to iterate through the data.  
  TestAtof(100, &data);

  double strtod_rate = Benchmark::Measure(TestStrtod, &data);
  double atof_rate = Benchmark::Measure(TestAtof, &data);
  double impala_rate = Benchmark::Measure(TestImpala, &data);

  cout << "Strtod Rate (per ms): " << strtod_rate << endl;
  cout << "Atof Rate (per ms): " << atof_rate << endl;
  cout << "Impala Rate (per ms): " << impala_rate << endl;

  return 0;
}

