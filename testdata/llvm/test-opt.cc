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

// This is a test-only file.  It was used to generate test-opt.bc
// which is used by the unit test to test optimization passes.

static const int m = 1;

static int transform(int d) {
  return d + m;
}

int loop_null(int* data, int num) {
  int res = 0;
  for (int i = 0; i < num; i++) {
    // Expected to be optimized out.
    if (i % 2 == 0) {
      res += transform(data[i]);
    }
    if (i % 2 == 0) {
      res -= transform(data[i]);
    }
  }
  return res;
}
