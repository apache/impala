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

#include <cstdio>  // file stuff
#include <cstdlib>  // rand
#include <errno.h>
#include <pthread.h>
#include <time.h>

#include "util/redactor.h"
#include "util/redactor-test-utils.h"

namespace impala {

TempRulesFile::TempRulesFile(const std::string& contents)
  : name_("/tmp/rules_XXXXXX"),
    deleted_(false) {
  int fd = mkstemp(&name_[0]);
  if (fd == -1) {
    std::cout << "Error creating temp file; " << strerror(errno) << std::endl;
    abort();
  }
  if (close(fd) != 0) {
    std::cout << "Error closing temp file; " << strerror(errno) << std::endl;
    abort();
  }
  OverwriteContents(contents);
}

void TempRulesFile::Delete() {
  if (deleted_) return;
  deleted_ = true;
  if (remove(name()) != 0) {
    std::cout << "Error deleting temp file; " << strerror(errno) << std::endl;
    abort();
  }
}

void TempRulesFile::OverwriteContents(const std::string& contents) {
  FILE* handle = fopen(name(), "w");
  if (handle == NULL) {
    std::cout << "Error creating temp file; " << strerror(errno) << std::endl;
    abort();
  }
  int status = fputs(contents.c_str(), handle);
  if (status < 0) {
    std::cout << "Error writing to temp file; " << strerror(errno) << std::endl;
    abort();
  }
  status = fclose(handle);
  if (status != 0) {
    std::cout << "Error closing temp file; " << strerror(errno) << std::endl;
    abort();
  }
}

unsigned int RandSeed() {
  struct timespec now;
  clock_gettime(CLOCK_REALTIME, &now);
  return now.tv_nsec + pthread_self();
}

/// Randomly fills the contents of 'string' up to the given length.
void RandomlyFillString(char* string, const int length) {
  ASSERT_GT(length, 0);
  unsigned int rand_seed = RandSeed();
  int char_count = static_cast<int>('~') - static_cast<int>(' ') + 1;
  for (int i = 0; i < length - 1; ++i) {
    string[i] = ' ' + rand_r(&rand_seed) % char_count;
  }
  string[length - 1] = '\0';
}

void AssertErrorMessageContains(const std::string& message, const char* expected) {
  ASSERT_TRUE(message.find(expected) != std::string::npos)
      << "Expected substring <<" << expected << ">> is not in <<" << message << ">>";
}

void AssertRedactedEquals(const char* message, const char* expected) {
  std::string temp(message);
  Redact(&temp);
  ASSERT_EQ(expected, temp);

  /// Test the signature with the 'changed' argument.
  temp = std::string(message);
  bool changed = false;
  Redact(&temp, &changed);
  ASSERT_EQ(expected, temp);
  ASSERT_EQ(temp == message, !changed);
}

void AssertUnredacted(const char* message) {
  AssertRedactedEquals(message, message);
}

}
