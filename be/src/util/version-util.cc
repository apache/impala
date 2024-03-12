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

#include "util/version-util.h"

#include "kudu/util/version_util.h"

namespace kudu {

bool operator<(const Version& lhs, const Version& rhs) {
  bool num_compare = lhs.major < rhs.major || lhs.minor < rhs.minor
      || lhs.maintenance < rhs.maintenance;

  if (num_compare) {
    return true;
  }

  if (lhs.extra_delimiter == rhs.extra_delimiter) {
    return lhs.extra < rhs.extra;
  }

  return lhs.extra_delimiter.has_value();
}

bool operator<=(const Version& lhs, const Version& rhs) {
  return lhs == rhs || lhs < rhs;
}

bool operator>(const Version& lhs, const Version& rhs) {
  return !(lhs <= rhs);
}

bool operator!=(const Version& lhs, const Version& rhs) {
  return !(lhs == rhs);
}

} // namespace kudu

namespace impala {

kudu::Version ConstructVersion(int maj, int min, int maint) {
  kudu::Version v;

  v.major = maj;
  v.minor = min;
  v.maintenance = maint;

  return v;
}

} // namespace impala
