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

#ifndef IMPALA_TESTUTIL_RAND_UTIL_H_
#define IMPALA_TESTUTIL_RAND_UTIL_H_

#include <cstdint>
#include <cstdlib>
#include <filesystem>
#include <random>
#include <vector>

#include <gtest/gtest.h>

#include "common/logging.h"

namespace impala {

/// Test helpers for randomised tests.
class RandTestUtil {
 public:
  /// Seed 'rng' with a seed either from the environment variable 'env_var' or the
  /// random device of the platform.
  template <typename RandomEngine>
  static void SeedRng(const char* env_var, RandomEngine* rng) {
    const char* seed_str = getenv(env_var);
    int64_t seed;
    if (seed_str != nullptr) {
      seed = atoi(seed_str);
    } else {
      seed = std::random_device()();
    }
    LOG(INFO) << "Random seed (overridable with " << env_var << "): " << seed;
    rng->seed(seed);
  }

  /// Create 'num_threads' rngs, seeded using 'seed_rng'. Used when we want each thread
  /// to have its own RNG (since they are not thread-safe).
  static vector<std::mt19937> CreateThreadLocalRngs(
      int num_threads, std::mt19937* seed_rng) {
    vector<std::mt19937> rngs(num_threads);
    for (std::mt19937& rng : rngs) rng.seed((*seed_rng)());
    return rngs;
  }

  /// Create a random temporary directory with the given prefix returning the path to the
  /// newly created directory. If 'rm_on_destruction' is true, the directory will be
  /// removed when the program exits.
  static std::filesystem::path CreateRandomTempDir(const std::string_view& prefix,
      const bool rm_on_destruction = true) {
    std::random_device rd;
    std::minstd_rand gen(rd());
    std::uniform_int_distribution<> distrib(100000, 999999);
    std::filesystem::path tmpl = std::filesystem::temp_directory_path()
        /= std::string(prefix) + "-" + std::to_string(distrib(gen));

    EXPECT_TRUE(std::filesystem::create_directory(tmpl))
        << "Failed to create temp directory: " << tmpl;

    if (rm_on_destruction) {
      if (temp_dirs_to_rm_.empty()) {
        // Register the cleanup function to remove temp directories at program exit.
        std::atexit([]() {
          for (const auto& dir : temp_dirs_to_rm_) {
            std::error_code ec;
            std::filesystem::remove_all(dir, ec);
            if (ec) {
              LOG(WARNING) << "Failed to remove temp directory: " << dir
                           << ", error: " << ec.message();
            }
          }
        });
      }
      temp_dirs_to_rm_.emplace_back(tmpl);
    }

    return tmpl;
  }

 private:
   inline static std::vector<std::filesystem::path> temp_dirs_to_rm_;
}; // class RandTestUtil

} // namespace impala

#endif
