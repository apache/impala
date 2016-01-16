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

#include "util/auth-util.h"

using namespace std;

namespace impala {

  const string& GetEffectiveUser(const TSessionState& session) {
    if (session.__isset.delegated_user && !session.delegated_user.empty()) {
      return session.delegated_user;
    }
    return session.connected_user;
  }

}
