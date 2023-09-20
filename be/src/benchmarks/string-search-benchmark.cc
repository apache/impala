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
#include "util/benchmark.h"
#include "util/cpu-info.h"
#include "runtime/string-search.h"
#include "experiments/string-search-sse.h"

#include "common/names.h"

using namespace impala;

// Benchmark tests for string search.  This is probably a science of its own
// but we'll run some simple tests.  (We can't use libc strstr because our
// strings are not null-terminated and also, it's not that fast).
// Results:
// String Search:        Function                Rate          Comparison
// ----------------------------------------------------------------------
//                         Python               81.93                  1X
//                           LibC               262.2              3.201X
//            Null Terminated SSE               212.4              2.592X
//        Non-null Terminated SSE               139.6              1.704X

struct TestData {
  vector<StringValue> needles;
  vector<StringValue> haystacks;
  int matches;
  vector<string> strings;       // Memory store for allocated strings
};

void TestLibc(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    data->matches = 0;
    for (int n = 0; n < data->needles.size(); ++n) {
      char* needle = data->needles[n].Ptr();
      for (int iters = 0; iters < 10; ++iters) {
        for (int h = 0; h < data->haystacks.size(); ++h) {
          if (strstr(data->haystacks[h].Ptr(), needle) != NULL) {
            ++data->matches;
          }
        }
      }
    }
  }
}

void TestPython(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    data->matches = 0;
    for (int n = 0; n < data->needles.size(); ++n) {
      StringSearch needle(&(data->needles[n]));
      for (int iters = 0; iters < 10; ++iters) {
        for (int h = 0; h < data->haystacks.size(); ++h) {
          if (needle.Search(&(data->haystacks[h])) != -1) {
            ++data->matches;
          }
        }
      }
    }
  }
}

void TestImpalaNullTerminated(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    data->matches = 0;
    for (int n = 0; n < data->needles.size(); ++n) {
      // Exercise the null-terminated path.
      StringSearchSSE needle(data->needles[n].Ptr());
      for (int iters = 0; iters < 10; ++iters) {
        for (int h = 0; h < data->haystacks.size(); ++h) {
          if (needle.Search(data->haystacks[h]) != -1) {
            ++data->matches;
          }
        }
      }
    }
  }
}

void TestImpalaNonNullTerminated(int batch_size, void* d) {
  TestData* data = reinterpret_cast<TestData*>(d);
  for (int i = 0; i < batch_size; ++i) {
    data->matches = 0;
    for (int n = 0; n < data->needles.size(); ++n) {
      // Exercise the non null-terminated path.
      StringSearchSSE needle(&(data->needles[n]));
      for (int iters = 0; iters < 10; ++iters) {
        for (int h = 0; h < data->haystacks.size(); ++h) {
          if (needle.Search(data->haystacks[h]) != -1) {
            ++data->matches;
          }
        }
      }
    }
  }
}

// This should be tailored to represent the data we expect.
// Some algos are better suited for longer vs shorter needles, finding the string vs.
// not finding the string, etc.
// For now, pick data that mimicks the grep data set.
void InitTestData(TestData* data) {
  vector<string> needles;
  vector<string> haystacks;

  needles.push_back("xyz");

  // From a random password generator:
  // https://www.grc.com/passwords.htm
  haystacks.push_back("d1h2POju5AG3zCxiBNKBxdzLdW7VfPkgvHDRLK2o78pGu4eywZ5mmmsV1LscqIH");
  haystacks.push_back("Xv7rmDx3ksECRPg94mPLI1gQjRIviZNINZwbDsvcyNAf7q4pzGQ6JHUqYHxFk0x");
  haystacks.push_back("cI8f2V9cYjmE70ruUeST9Arty7B8iKLiF4DaXhY4nF7OmXZOKRBVPd5cmjxiDRw");
  haystacks.push_back("3cb7QCWJ7QzNSwaXQQIByo7is1b3YPTx9u0ZmRV0IJEKJE2z2oMaKgzeA1ny7hx");
  haystacks.push_back("teRFhdOc5HrGvXUo1yGZSUc46hCVLjVzGaK37vfGAsuCpiji2R4YQ7Y4FN2O3lu");
  haystacks.push_back("xecRm4HYLTfhQgPQHTIhWQKNGJcAUYCziDJQlbAitVHC9ClbbcEkVrXsFSpVrx9");
  haystacks.push_back("M1k4su3sxpcBl8SQgnF6LOD1fNjTrQQW84vLofF39IbY4gTVpsHIlon8mbJ74D7");
  haystacks.push_back("dMROjEop61WoHNSegkiw7o1uIM5xgccf9SRhIEOnq9CoM7e0KatpjAmGLlANggK");
  haystacks.push_back("OIPM3iTT40sSkCTfDOjK4BSHZvV0xazTXarwYhyQEu5aGn7XeCABezJhbO1X8rr");
  haystacks.push_back("0YyLBoITUS7Mxfo3o44j4eEdWqwx6Um5EkUzIo7c0ETYfnkcaHsYcpT5Xn3coyE");
  haystacks.push_back("LzOFXp8ekVkeBy6Br9rRefzDDA30SLkvV3gGochoKGUkS0oj9fANBfOSBeyNypm");
  haystacks.push_back("uYDMZ4SNIh6XwBQcGCS3AQ5h5c2iyaPS70r4emPheC3hQhM84zwsMovByLNecTW");
  haystacks.push_back("EFY8GtkUkXRJQr3ZjANKhUFXTmnA2S4V6hTKeeskQhlUA65lTCO0PpyZxL8kyVE");
  haystacks.push_back("fgU8K01JtTn3UldvO9jAywDEN4HW5PwCFEJ4pwSwHfec6HsBN5rzzhQ2Tn4QDxE");
  haystacks.push_back("TMLE6UzW3CUUogfRFjZooP1eCZn05bI6NsWL67fOmwYhZND1nuAASG0ZhGFBdZH");

  // Contains the key in the beginning/middle/end
  haystacks.push_back("of9VxyzzWca0GBs8CrFsDIvi8yWCkRTRRL7X4TlMhPfRAWPOZfRVwybJoYQhslJ");
  haystacks.push_back("iEO7zz7wWm4LmvumisOwPRUzv9xyzhI7omKDlLYM7Vao9pggACVgDJBhAzDkcJp");
  haystacks.push_back("tQJzS0SiXRElwq1QEBhy0gGGii9xAcQbTIrt4QcGMViyOx4lfZ73zZ5XHxyzk1V");
  haystacks.push_back("tQJzS0SiXRElwq1QEBhy0gGGii9xAcQbTIrt4QcGMViyOx4lfZ73zZ5XHyzaxyz");

  for (int i = 0; i < needles.size(); ++i) {
    StringValue v(const_cast<char*>(needles[i].c_str()), needles[i].size());
    data->needles.push_back(v);
    data->strings.push_back(needles[i]);
  }
  for (int i = 0; i < haystacks.size(); ++i) {
    StringValue v(const_cast<char*>(haystacks[i].c_str()), haystacks[i].size());
    data->haystacks.push_back(v);
    data->strings.push_back(haystacks[i]);
  }
}

int main(int argc, char **argv) {
  CpuInfo::Init();
  cout << Benchmark::GetMachineInfo() << endl;

  TestData data;
  InitTestData(&data);

  Benchmark suite("String Search");
  suite.AddBenchmark("Python", TestPython, &data);
  suite.AddBenchmark("LibC", TestLibc, &data);
  suite.AddBenchmark("Null Terminated SSE", TestImpalaNullTerminated, &data);
  suite.AddBenchmark("Non-null Terminated SSE", TestImpalaNonNullTerminated, &data);
  cout << suite.Measure();

  return 0;
}
