// Copyright 2012 Cloudera Inc.
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

#include "util/decimal-util.h"
#include "runtime/types.h"

namespace impala {

int128_t DecimalUtil::MAX_UNSCALED_DECIMAL;

void DecimalUtil::InitMaxUnscaledDecimal() {
  // TODO: is there a better way to do this?
  MAX_UNSCALED_DECIMAL = 0;
  for (int i = 0; i < ColumnType::MAX_PRECISION; ++i) {
    MAX_UNSCALED_DECIMAL *= 10;
    MAX_UNSCALED_DECIMAL += 9;
  }
}

}
