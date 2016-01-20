// Copyright 2016 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include <stdint.h>

#include <iostream>
#include <vector>

#include <x86intrin.h>

#include "util/benchmark.h"
#include "util/cpu-info.h"
#include "util/sse-util.h"

using namespace std;
using namespace impala;

// Test hash functions that take integers as arguments and produce integers as the result.
//
// Machine Info: Intel(R) Core(TM) i7-4790 CPU @ 3.60GHz
// 32 -> 32:             Function     Rate (iters/ms)          Comparison
// ----------------------------------------------------------------------
//                       Jenkins2               14.06                  1X
//                       Jenkins1               16.97              1.207X
//                        MultRot               19.14              1.361X
//               MultiplyAddShift               21.21              1.509X
//                  MultiplyShift                25.3              1.799X
//                            CRC               25.53              1.816X


// Make a random uint32_t, avoiding the absent high bit and the low-entropy low bits
// produced by rand().
uint32_t MakeRandU32() {
  uint32_t result = (rand() >> 8) & 0xffff;
  result <<= 16;
  result |= (rand() >> 8) & 0xffff;
  return result;
}

// Almost universal hashing, M. Dietzfelbinger, T. Hagerup, J. Katajainen, and M.
// Penttonen, "A reliable randomized algorithm for the closest-pair problem".
uint32_t MultiplyShift(uint32_t x) {
  static const uint32_t m = 0x61eaf8e9u;
  return x*m;
}

// 2-independent hashing. M. Dietzfelbinger, "Universal hashing and k-wise independent
// random variables via integer arithmetic without primes"
uint32_t MultiplyAddShift(uint32_t x) {
  static const uint64_t m = 0xa1f1bd3e020b4be0ull, a = 0x86b0426193d86e66ull;
  return (static_cast<uint64_t>(x) * m + a) >> 32;
}

// From http://web.archive.org/web/20071223173210/http://www.concentric.net/~Ttwang/tech/inthash.htm:
int32_t Jenkins1(int32_t x) {
  x = ~x + (x << 15);  // x = (x << 15) - x - 1;
  x = x ^ _rotr(x, 12);
  x = x + (x << 2);
  x = x ^ _rotr(x, 4);
  x = x * 2057;  // x = (x + (x << 3)) + (x << 11);
  x = x ^ _rotr(x, 16);
  return x;
}

// From http://web.archive.org/web/20071223173210/http://www.concentric.net/~Ttwang/tech/inthash.htm:
uint32_t Jenkins2(uint32_t a) {
  a = (a + 0x7ed55d16) + (a << 12);
  a = (a ^ 0xc761c23c) ^ (a >> 19);
  a = (a + 0x165667b1) + (a << 5);
  a = (a + 0xd3a2646c) ^ (a << 9);
  a = (a + 0xfd7046c5) + (a << 3);
  a = (a ^ 0xb55a4f09) ^ (a >> 16);
  return a;
}

// From http://web.archive.org/web/20071223173210/http://www.concentric.net/~Ttwang/tech/inthash.htm:
int32_t MultRot(int32_t key) {
  static const int32_t c2 = 0x27d4eb2d;  // a prime or an odd constant
  key = (key ^ 61) ^ _rotr(key, 16);
  key = key + (key << 3);
  key = key ^ _rotr(key, 4);
  key = key * c2;
  key = key ^ _rotr(key, 15);
  return key;
}

uint32_t CRC(uint32_t x) {
  return SSE4_crc32_u32(x,0xab8ce2abu);
}

template<typename T, T (*HASH)(T)>
void Run(int batch_size, void* data) {
  vector<T>* d = reinterpret_cast<vector<T>*>(data);
  for (int i = 0; i < batch_size; ++i) {
    for (int j = 0; j < d->size(); ++j) {
      (*d)[j] = HASH((*d)[j]);
    }
  }
}

int main() {
  CpuInfo::Init();
  cout << endl
       << Benchmark::GetMachineInfo() << endl;

  vector<uint32_t> ud(1 << 15);
  for (size_t i = 0; i < ud.size(); ++i) {
    ud[i] = MakeRandU32();
  }

  Benchmark suite("32 -> 32");

#define BENCH(T,x) suite.AddBenchmark(#x, Run<T, x>, &ud)
  BENCH(uint32_t, Jenkins2);
  BENCH(int32_t, Jenkins1);
  BENCH(int32_t, MultRot);
  BENCH(uint32_t, MultiplyAddShift);
  BENCH(uint32_t, MultiplyShift);
  BENCH(uint32_t, CRC);
#undef BENCH

  cout << suite.Measure() << endl;
}
