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


#ifndef IMPALA_UTIL_AUTH_UTIL_H
#define IMPALA_UTIL_AUTH_UTIL_H

#include <string>

#include "gen-cpp/ImpalaInternalService_types.h"

namespace impala {

  /// Returns a reference to the "effective user" from the specified session. Queries
  /// are run and authorized on behalf of the effective user. When a delegated_user is
  /// specified (is not empty), the effective user is the delegated_user. This is because
  /// the connected_user is acting as a "proxy user" for the delegated_user. When
  /// delegated_user is empty, the effective user is the connected user.
  const std::string& GetEffectiveUser(const TSessionState& session);

} // namespace impala
#endif
