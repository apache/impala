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

#ifndef IMPALA_TESTUTIL_SCOPED_FLAG_SETTER_H
#define IMPALA_TESTUTIL_SCOPED_FLAG_SETTER_H

namespace impala {

/// Temporarily sets a flag for the duration of its scope, then resets the flag to its
/// original value upon destruction.
//
/// Example (pre-condition: FLAGS_my_string_flag == "world"):
/// {
///   auto s = ScopedFlagSetter<string>::Make(&FLAGS_my_string_flag, "hello");
///   // ... FLAGS_my_string_flag == "hello" for entire scope
/// }
/// // After destruction of 's', FLAGS_my_string_flag == "world" again.
template <typename T>
struct ScopedFlagSetter {
  static ScopedFlagSetter<T> Make(T* f, const T& new_val) {
    return ScopedFlagSetter(f, new_val);
  }

  ~ScopedFlagSetter() { *flag = old_val; }

 private:
  ScopedFlagSetter(T* f, T new_val) {
    flag = f;
    old_val = *f;
    *f = new_val;
  }

  T* flag;
  T old_val;
};
}

#endif
