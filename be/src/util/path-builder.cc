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

#include "util/path-builder.h"

#include <sstream>
#include <stdlib.h>

#include "common/names.h"

using namespace impala;

const char* PathBuilder::impala_home_;

void PathBuilder::LoadImpalaHome() {
  if (impala_home_ != NULL) return;
  impala_home_ = getenv("IMPALA_HOME");
}

void PathBuilder::GetFullPath(const string& path, string* full_path) {
  LoadImpalaHome();
  stringstream s;
  s << impala_home_ << "/" << path;
  *full_path = s.str();
}

void PathBuilder::GetFullBuildPath(const string& path, string* full_path) {
  LoadImpalaHome();
  stringstream s;
#ifdef NDEBUG
  s << impala_home_ << "/be/build/release/" << path;
#else
  s << impala_home_ << "/be/build/debug/" << path;
#endif
  *full_path = s.str();
}
