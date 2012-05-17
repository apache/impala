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

// Benchmark for doing atoi.  This benchmark compares various implementations
// to convert string to int32s.  The data is mostly positive, relatively small
// numbers.
// Results:
//   Strtol Rate (per ms): 57.4257
//   Atoi Rate (per ms): 57.7821
//   Impala Rate (per ms): 147.268
//   Impala Unsafe Rate (per ms): 200.182
//   Impala Unrolled Rate (per ms): 167.628
//   Impala Cased Rate (per ms): 169.307
#define VALIDATE 1

#if VALIDATE
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
  vector<int32_t> result;
};

void AddTestData(TestData* data, const string& input) {
  data->memory.push_back(input);
  const string& str = data->memory.back();
  data->data.push_back(StringValue(const_cast<char*>(str.c_str()), str.length()));
}

void AddTestData(TestData* data, int n, int32_t min = -10, int32_t max = 10) {
  for (int i = 0; i < n; ++i) {
    double val = rand();
    val /= RAND_MAX;
    val = static_cast<int32_t>((val * (max - min)) + min);
    stringstream ss;
    ss << val;
    AddTestData(data, ss.str());
  }
}

#define DIGIT(c) (c -'0')

inline int32_t AtoiUnsafe(char* s, int len) {
  int32_t val = 0;
  bool negative = false;
  int i = 0;
  switch (*s) {
    case '-': negative = true;
    case '+': ++i;
  }

  for (; i < len; ++i) {
    val = val * 10 + DIGIT(s[i]);
  }

  return negative ? -val : val;
}

inline int32_t AtoiUnrolled(char* s, int len) {
  if (LIKELY(len <= 8)) {
    int32_t val = 0;
    bool negative = false;
    switch (*s) {
      case '-': negative = true;
      case '+': --len, ++s;
    }

    switch(len) {
      case 8:
        val += (DIGIT(s[len - 8])) * 10000;
      case 7:
        val += (DIGIT(s[len - 7])) * 10000;
      case 6:
        val += (DIGIT(s[len - 6])) * 10000;
      case 5:
        val += (DIGIT(s[len - 5])) * 10000;
      case 4:
        val += (DIGIT(s[len - 4])) * 1000;
      case 3:
        val += (DIGIT(s[len - 3])) * 100;
      case 2:
        val += (DIGIT(s[len - 2])) * 10;
      case 1:
        val += (DIGIT(s[len - 1]));
    }
    return negative ? -val : val;
  } else {
    return AtoiUnsafe(s, len);
  }
}

inline int32_t AtoiCased(char* s, int len) {
  if (LIKELY(len <= 5)) {
    int32_t val = 0;
    bool negative = false;
    switch (*s) {
      case '-': negative = true;
      case '+': --len, ++s;
    }

    switch(len) {
      case 5:
        val = DIGIT(s[0])*10000 + DIGIT(s[1])*1000 + DIGIT(s[2])*100 + 
              DIGIT(s[3])*10 + DIGIT(s[4]);
        break;
      case 4:
        val = DIGIT(s[0])*1000 + DIGIT(s[1])*100 + DIGIT(s[2])*10 + DIGIT(s[3]);
        break;
      case 3:
        val = DIGIT(s[0])*100 + DIGIT(s[1])*10 + DIGIT(s[2]);
        break;
      case 2:
        val = DIGIT(s[0])*10 + DIGIT(s[1]);
        break;
      case 1:
        val = DIGIT(s[0]);
        break;
    }
    return negative ? -val : val;
  } else {
    return AtoiUnsafe(s, len);
  }
}

void TestAtoi(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      data->result[j] = atoi(data->data[j].ptr);
    }
  }
}

void TestStrtol(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      data->result[j] = strtol(data->data[j].ptr, NULL, 10);
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
      int32_t val = StringParser::StringToInt<int32_t>(str.ptr, str.len, &dummy);
      VALIDATE_RESULT(val, data->result[j], str.ptr);
      data->result[j] = val;
    }
  }
}

void TestImpalaUnsafe(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      const StringValue& str = data->data[j];
      int32_t val = AtoiUnsafe(str.ptr, str.len);
      VALIDATE_RESULT(val, data->result[j], str.ptr);
      data->result[j] = val;
    }
  }
}

void TestImpalaUnrolled(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      const StringValue& str = data->data[j];
      int32_t val = AtoiUnrolled(str.ptr, str.len);
      VALIDATE_RESULT(val, data->result[j], str.ptr);
      data->result[j] = val;
    }
  }
}

void TestImpalaCased(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    int n = data->data.size();
    for (int j = 0; j < n; ++j) {
      const StringValue& str = data->data[j];
      int32_t val = AtoiCased(str.ptr, str.len);
      VALIDATE_RESULT(val, data->result[j], str.ptr);
      data->result[j] = val;
    }
  }
}

int main(int argc, char **argv) {
  TestData data;

  // Most data is probably positive
  AddTestData(&data, 1000, -5, 1000);

  data.result.resize(data.data.size());

  // Run a warmup to iterate through the data.  
  TestAtoi(1000, &data);

  double strtol_rate = Benchmark::Measure(TestStrtol, &data);
  double atoi_rate = Benchmark::Measure(TestAtoi, &data);
  double impala_rate = Benchmark::Measure(TestImpala, &data);
  double unsafe_rate = Benchmark::Measure(TestImpalaUnsafe, &data);
  double unrolled_rate = Benchmark::Measure(TestImpalaUnrolled, &data);
  double cased_rate = Benchmark::Measure(TestImpalaCased, &data);

  cout << "Strtol Rate (per ms): " << strtol_rate << endl;
  cout << "Atoi Rate (per ms): " << atoi_rate << endl;
  cout << "Impala Rate (per ms): " << impala_rate << endl;
  cout << "Impala Unsafe Rate (per ms): " << unsafe_rate << endl;
  cout << "Impala Unrolled Rate (per ms): " << unrolled_rate << endl;
  cout << "Impala Cased Rate (per ms): " << cased_rate << endl;

  return 0;
}

