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
#ifndef IMPALA_REDACTOR_TEST_UTILS_H
#define IMPALA_REDACTOR_TEST_UTILS_H

#include <string>

#include <gtest/gtest.h>

namespace impala {

/// Utility class for creating a redaction config file that will be automatically deleted
/// upon test completion.
class TempRulesFile {
 public:
  // Creates a temporary file with the specified contents.
  TempRulesFile(const std::string& contents);

  ~TempRulesFile() { Delete(); }

  // Delete this temporary file
  void Delete();

  // Overwrite the temporary file with the specified contents.
  void OverwriteContents(const std::string& contents);

  /// Returns the absolute path to the file.
  const char* name() const { return name_.c_str(); }

 private:
  std::string name_;
  bool deleted_;
};

// Produces a random seed based on the current time and the thread id.
unsigned int RandSeed();

/// Randomly fills the contents of 'string' up to the given length.
void RandomlyFillString(char* string, const int length);

/// Assert 'message' contains 'expected'
void AssertErrorMessageContains(const std::string& message, const char* expected);

/// Redact the 'message' and assert that it matches 'expected'
void AssertRedactedEquals(const char* message, const char* expected);

/// Assert that redaction does nothing for the specified message
void AssertUnredacted(const char* message);

/// Putting these assertion utilities above into functions messes up failure messages
/// such that failures appear to be coming from this file instead of from the file
/// that called the utility assertion. Using a "SCOPED_TRACE" adds the location of the
/// caller to the error message.
#define SCOPED_ASSERT(assertion) { \
    SCOPED_TRACE(""); \
    assertion; \
    if (HasFatalFailure()) return; \
  }

#define ASSERT_ERROR_MESSAGE_CONTAINS(error, expected) \
  SCOPED_ASSERT(AssertErrorMessageContains(error, expected))

#define ASSERT_REDACTED_EQ(actual, expected) \
  SCOPED_ASSERT(AssertRedactedEquals(actual, expected))

#define ASSERT_UNREDACTED(string) SCOPED_ASSERT(AssertUnredacted(string))

}

#endif
