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

#include <iostream>

#include <boost/optional.hpp>

#include "common/status.h"
#include "util/benchmark.h"
#include "util/cpu-info.h"

#include "common/names.h"

#define BODY_ONE(body) do { \
    body                    \
  } while (false);

#define BODY_TEN(body) BODY_ONE(body) BODY_ONE(body) BODY_ONE(body) \
  BODY_ONE(body) BODY_ONE(body) BODY_ONE(body) BODY_ONE(body) \
  BODY_ONE(body) BODY_ONE(body) BODY_ONE(body)

#define BODY_HUNDRED(body) BODY_TEN(body) BODY_TEN(body) BODY_TEN(body) \
  BODY_TEN(body) BODY_TEN(body) BODY_TEN(body) BODY_TEN(body) BODY_TEN(body) \
  BODY_TEN(body) BODY_TEN(body)

#define BODY_1K(body) BODY_HUNDRED(body) BODY_HUNDRED(body) BODY_HUNDRED(body) \
  BODY_HUNDRED(body) BODY_HUNDRED(body) BODY_HUNDRED(body) BODY_HUNDRED(body) \
  BODY_HUNDRED(body) BODY_HUNDRED(body) BODY_HUNDRED(body)


// Machine Info: Intel(R) Core(TM) i7-4770 CPU @ 3.40GHz
// status:               Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//              Call Status::OK()           9.555e+08                  1X
//      Call static Status::Error           4.515e+07            0.04725X
//    Call Status(Code, 'string')           9.873e+06            0.01033X
//             Call w/ Assignment           5.422e+08             0.5674X
//            Call Cond Branch OK           5.941e+06           0.006218X
//         Call Cond Branch ERROR           7.047e+06           0.007375X
//  Call Cond Branch Bool (false)           1.914e+10              20.03X
//   Call Cond Branch Bool (true)           1.491e+11                156X
// Call Cond Boost Optional (true)          3.935e+09              4.118X
// Call Cond Boost Optional (false)         2.147e+10              22.47X

using namespace impala;

struct data {
  bool good;
};

Status CallOK() { return Status::OK(); }

Status global;

void TestCallOK(int batch_size, void* d) {
  BODY_1K( global = CallOK(); );
}

Status CallStaticError() { return Status::CANCELLED; }

void TestCallStaticError(int batch_size, void*) {
  BODY_1K( global = CallStaticError(); );
}

Status CallCreateError() { return Status(TErrorCode::GENERAL, "Some Error"); }

void TestCallCreateError(int batch_size, void*) {
  BODY_1K( global = CallCreateError(); );
}

Status CallAssignment() {
  Status s = CallOK();
  return s;
}

void TestCallAssignment(int batch_size, void*) {
  BODY_1K( global = CallAssignment(); );
}

Status CallCondBranch(bool good) {
  Status s = Status::OK();
  if (!good) s = Status(TErrorCode::GENERAL, "Some Error");
  return s;
}

void TestCallCondBranch(int batch_size, void* d) {
  bool g = ((data*)d)->good;
  BODY_1K( global = CallCondBranch(g); );
}

bool global_bool = true;

bool CallBool(bool good) {
  if (good) return true;
  return false;
}

void TestCallCondBool(int batch_size, void* d) {
  bool g = ((data*)d)->good;
  BODY_1K( global_bool = CallBool(g); );
}

struct large_type {
  long val1;
  long val2;
  long val3;
  large_type(long a, long b, long c): val1(a), val2(b), val3(c) {}
};

large_type global_large(0l, 0l, 0l);

boost::optional<large_type> CallOptional(bool good) {
  if (good) return large_type(0l, 3l, 9l);
  return boost::none;
}

void TestCallOptional(int batch_size, void* d) {
  bool g = ((data*)d)->good;
  BODY_1K( if (auto v = CallOptional(g)) global_large = v.get(); );
}


int main(int argc, char** argv) {
  CpuInfo::Init();
  cout << Benchmark::GetMachineInfo() << endl;

  data d = {true};

  Benchmark suite("status");
  int baseline = suite.AddBenchmark("Call Status::OK()", TestCallOK, &d, -1);
  suite.AddBenchmark("Call static Status::Error", TestCallStaticError, &d, baseline);
  suite.AddBenchmark("Call Status(Code, 'string')", TestCallCreateError, &d, baseline);
  suite.AddBenchmark("Call w/ Assignment", TestCallAssignment, &d, baseline);

  d.good = true;
  suite.AddBenchmark("Call Cond Branch OK", TestCallCondBranch, &d, baseline);
  d.good = false;
  suite.AddBenchmark("Call Cond Branch ERROR", TestCallCondBranch, &d, baseline);

  d.good = false;
  suite.AddBenchmark("Call Cond Branch Bool (false)", TestCallCondBool, &d, baseline);
  d.good = true;
  suite.AddBenchmark("Call Cond Branch Bool (true)", TestCallCondBool, &d, baseline);
  d.good = true;
  suite.AddBenchmark("Call Cond Boost Optional (true)", TestCallOptional, &d, baseline);
  d.good = false;
  suite.AddBenchmark("Call Cond Boost Optional (false)", TestCallOptional, &d, baseline);

  cout << suite.Measure() << endl;
}
