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

#ifndef IMPALA_UTIL_MINIDUMP_H
#define IMPALA_UTIL_MINIDUMP_H

#include "common/status.h"

namespace impala {

/// Register a minidump handler to generate breakpad minidumps to path.
/// See https://chromium.googlesource.com/breakpad/breakpad/ for more details.
Status RegisterMinidump(const char* cmd_line_path);

/// Test helper to temporarily enable or disable minidumps, for example during death
/// tests that deliberately trigger DCHECKs. Returns true if minidumps were previously
/// enabled or false otherwise.
bool EnableMinidumpsForTest(bool enabled);

/// Checks the number of minidump files and removes the oldest ones to maintain an upper
/// bound on the number of files.
void CheckAndRotateMinidumps(int max_minidumps);
}

#endif
