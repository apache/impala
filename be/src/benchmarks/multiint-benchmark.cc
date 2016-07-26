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

#include <iomanip>
#include <iostream>
#include <sstream>

#include "util/benchmark.h"
#include "util/cpu-info.h"

#include "runtime/multi-precision.h"
#include "common/names.h"

// Benchmark to measure operations on different implementation of multi (i.e. > 8)
// byte integers.
// Machine Info: Intel(R) Core(TM) i7-2600 CPU @ 3.40GHz
// Add:                  Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                     int128_CPP            1.87e+04                  1X
//                   int128_Boost                2842             0.1519X
//                      int96_CPP                1289            0.06891X
//                          int64           2.381e+04              1.273X
//                  int128_Base1B            1.08e+04             0.5775X
//                        doubles           2.559e+04              1.368X
//
// Multiply:             Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                     int128_CPP           1.263e+04                  1X
//                   int128_Boost                4418             0.3497X
//                      int96_CPP                1220            0.09656X
//                          int64           2.913e+04              2.306X
//                        doubles           2.282e+04              1.806X
//
// Divide:               Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                     int128_CPP                1696                  1X
//                   int128_Boost                1112             0.6558X
//                      int96_CPP               561.7             0.3313X
//                          int64                2788              1.644X
//                         double                4565              2.692X


using std::max;
using std::numeric_limits;
using namespace impala;

// Multi byte ints encoded using a big base.
// The decimal value is:
// data[0] + data[1] * BASE^1 + data[2] * BASE^2 + ...
// This is not a full implementation of the functionality required.
template<uint32_t BASE>
struct BaseInt128 {
  uint32_t data[4]; // least significant first.

  BaseInt128(uint64_t v = 0) {
    memset(data, 0, sizeof(data));
    int idx = 0;
    while (v > 0) {
      data[idx++] = v % BASE;
      v /= BASE;
    }
  }

  BaseInt128& operator+=(const BaseInt128& rhs) {
    uint64_t r0 = data[0] + rhs.data[0];
    uint64_t r1 = data[1] + rhs.data[1];
    uint64_t r2 = data[2] + rhs.data[2];
    uint64_t r3 = data[3] + rhs.data[3];

    this->data[0] = static_cast<uint32_t>(r0);
    if (r0 > numeric_limits<uint32_t>::max()) ++r1;
    this->data[1] = static_cast<uint32_t>(r1);
    if (r1 > numeric_limits<uint32_t>::max()) ++r2;
    this->data[2] = static_cast<uint32_t>(r2);
    if (r2 > numeric_limits<uint32_t>::max()) ++r3;
    this->data[3] = static_cast<uint32_t>(r3);
    return *this;
  }

  string Print() const {
    stringstream ss;
    bool print_padded = false;
    for (int i = 3; i >= 0; --i) {
      if (data[i] == 0 && !print_padded) continue;
      if (print_padded) {
        ss << setw(9) << data[i];
      } else {
        ss << data[i];
      }
    }
    ss << data[0];
    return ss.str();
  }
};

// Prototype for int96 by doing the arithmetic as casts to int128. Minimal
// implementation to run benchmark.
struct int96 {
 public:
   int96() {
   }

   int96(__int128_t t) {
     memcpy(data, reinterpret_cast<uint8_t*>(&t), 12);
   }

   __int128_t to_int128() const {
     __int128_t r;
     // Fill in upper bits and bit shift down to sign extend
     memcpy(reinterpret_cast<uint8_t*>(&r) + 4, data, 12);
     r >>= 4;
     return r;
   }

   int96& operator+=(const int96& v) {
     __int128_t x = to_int128();
     __int128_t y = v.to_int128();
     __int128_t r = x + y;
     *this = int96(r);
     return *this;
   }

   int96& operator*=(const int96& v) {
     __int128_t x = to_int128();
     __int128_t y = v.to_int128();
     __int128_t r = x * y;
     *this = int96(r);
     return *this;
   }

   int96 operator/(const int96& v) const {
     __int128_t x = to_int128();
     __int128_t y = v.to_int128();
     __int128_t r = x / y;
     return int96(r);
   }

   int96 operator-() const {
     return int96(-to_int128());
   }

 private:
   // First (little endian) 12 bytes of int128_t
   uint8_t data[12];
};

typedef BaseInt128<1000000000> Base1BInt128;

struct TestData {
  // multi precision ints implemented with the boost library.
  vector<boost::multiprecision::int128_t> boost_add_ints;
  vector<boost::multiprecision::int128_t> boost_mult_ints;
  boost::multiprecision::int128_t boost_result;

  // multi precision ints as defined by the c++ extension
  vector<__int128_t> cpp_add_ints;
  vector<__int128_t> cpp_mult_ints;
  __int128_t cpp_result;

  vector<int96> cpp96_add_ints;
  vector<int96> cpp96_mult_ints;
  int96 cpp96_result;

  vector<int64_t> int64_ints;
  int64_t int64_result;

  vector<double> doubles;
  double double_result;

  vector<Base1BInt128> base1b_ints;
  Base1BInt128 base1b_result;
};

// Initialize test data. 1/4 will be negative. 1/2 will require more than 8 bytes.
void InitTestData(TestData* data, int n) {
  for (int i = 0; i < n; ++i) {
    data->boost_add_ints.push_back(boost::multiprecision::int128_t(i + 1));
    data->boost_mult_ints.push_back(boost::multiprecision::int128_t(i + 1));
    data->cpp_add_ints.push_back(__int128_t(i + 1));
    data->cpp_mult_ints.push_back(__int128_t(i + 1));
    data->cpp96_add_ints.push_back(__int128_t(i + 1));
    data->cpp96_mult_ints.push_back(__int128_t(i + 1));

    if (i % 2 == 0) {
      data->boost_add_ints[i] *= numeric_limits<int64_t>::max();
      data->cpp_add_ints[i] *= numeric_limits<int64_t>::max();
      data->cpp96_add_ints[i] *= numeric_limits<int64_t>::max();
    }
    if (i % 4 == 0) {
      data->boost_add_ints[i] = -data->boost_add_ints[i];
      data->cpp_add_ints[i] = -data->cpp_add_ints[i];
      data->cpp96_add_ints[i] = -data->cpp96_add_ints[i];

      data->boost_mult_ints[i] = -data->boost_mult_ints[i];
      data->cpp_mult_ints[i] = -data->cpp_mult_ints[i];
      data->cpp96_mult_ints[i] = -data->cpp96_mult_ints[i];
    }

    data->int64_ints.push_back(i + 1);
    data->base1b_ints.push_back(i + 1);
    data->doubles.push_back(i + 1);
  }
}

#define TEST_ADD(NAME, RESULT, VALS)\
  void NAME(int batch_size, void* d) {\
    TestData* data = reinterpret_cast<TestData*>(d);\
    for (int i = 0; i < batch_size; ++i) {\
      data->RESULT = 0;\
      for (int j = 0; j < data->VALS.size(); ++j) {\
        data->RESULT += data->VALS[j];\
      }\
    }\
  }

#define TEST_MULTIPLY(NAME, RESULT, VALS)\
  void NAME(int batch_size, void* d) {\
    TestData* data = reinterpret_cast<TestData*>(d);\
    for (int i = 0; i < batch_size; ++i) {\
      data->RESULT = 1;\
      for (int j = 0; j < data->VALS.size(); ++j) {\
        data->RESULT *= data->VALS[j];\
      }\
    }\
  }

#define TEST_DIVIDE(NAME, RESULT, VALS)\
  void NAME(int batch_size, void* d) {\
    TestData* data = reinterpret_cast<TestData*>(d);\
    for (int i = 0; i < batch_size; ++i) {\
      data->RESULT = 0;\
      for (int j = 0; j < data->VALS.size() - 1; ++j) {\
        data->RESULT += data->VALS[j + 1] / data->VALS[j];\
      }\
    }\
  }

TEST_ADD(TestBoostAdd, boost_result, boost_add_ints);
TEST_ADD(TestCppAdd, cpp_result, cpp_add_ints);
TEST_ADD(TestCpp96Add, cpp96_result, cpp96_add_ints);
TEST_ADD(TestBaseBillionAdd, base1b_result, base1b_ints);
TEST_ADD(TestInt64Add, int64_result, int64_ints);
TEST_ADD(TestDoubleAdd, double_result, doubles);

TEST_MULTIPLY(TestBoostMultiply, boost_result, boost_mult_ints);
TEST_MULTIPLY(TestCppMultiply, cpp_result, cpp_mult_ints);
TEST_MULTIPLY(TestCpp96Multiply, cpp96_result, cpp96_mult_ints);
TEST_MULTIPLY(TestInt64Multiply, int64_result, int64_ints);
TEST_MULTIPLY(TestDoubleMultiply, double_result, doubles);

TEST_DIVIDE(TestBoostDivide, boost_result, boost_mult_ints);
TEST_DIVIDE(TestCppDivide, cpp_result, cpp_mult_ints);
TEST_DIVIDE(TestCpp96Divide, cpp96_result, cpp96_mult_ints);
TEST_DIVIDE(TestInt64Divide, int64_result, int64_ints);
TEST_DIVIDE(TestDoubleDivide, double_result, doubles);

int main(int argc, char** argv) {
  CpuInfo::Init();
  cout << Benchmark::GetMachineInfo() << endl;

  TestData data;
  InitTestData(&data, 38); // 38! doesn't overflow int128.

  Benchmark add_suite("Add");
  add_suite.AddBenchmark("int128_CPP", TestCppAdd, &data);
  add_suite.AddBenchmark("int128_Boost", TestBoostAdd, &data);
  add_suite.AddBenchmark("int96_CPP", TestCpp96Add, &data);
  add_suite.AddBenchmark("int64", TestInt64Add, &data);
  add_suite.AddBenchmark("int128_Base1B", TestBaseBillionAdd, &data);
  add_suite.AddBenchmark("doubles", TestDoubleAdd, &data);
  cout << add_suite.Measure() << endl;

  Benchmark multiply_suite("Multiply");
  multiply_suite.AddBenchmark("int128_CPP", TestCppMultiply, &data);
  multiply_suite.AddBenchmark("int128_Boost", TestBoostMultiply, &data);
  multiply_suite.AddBenchmark("int96_CPP", TestCpp96Multiply, &data);
  multiply_suite.AddBenchmark("int64", TestInt64Multiply, &data);
  multiply_suite.AddBenchmark("doubles", TestDoubleMultiply, &data);
  cout << multiply_suite.Measure() << endl;

  Benchmark divide_suite("Divide");
  divide_suite.AddBenchmark("int128_CPP", TestCppDivide, &data);
  divide_suite.AddBenchmark("int128_Boost", TestBoostDivide, &data);
  divide_suite.AddBenchmark("int96_CPP", TestCpp96Divide, &data);
  divide_suite.AddBenchmark("int64", TestInt64Divide, &data);
  divide_suite.AddBenchmark("double", TestDoubleDivide, &data);
  cout << divide_suite.Measure() << endl;

  return 0;
}
