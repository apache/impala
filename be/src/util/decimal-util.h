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


#ifndef IMPALA_UTIL_DECIMAL_UTIL_H
#define IMPALA_UTIL_DECIMAL_UTIL_H

#include <ostream>
#include <string>
#include <boost/cstdint.hpp>

#include "runtime/types.h"

namespace impala {

class DecimalUtil {
 public:
  // TODO: do we need to handle overflow here or at a higher abstraction.
  template<typename T>
  static T MultiplyByScale(const T& v, const ColumnType& t) {
    DCHECK(t.type == TYPE_DECIMAL);
    return MultiplyByScale(v, t.scale);
  }

  template<typename T>
  static T MultiplyByScale(const T& v, int scale) {
    return v * GetScaleMultiplier<T>(scale);
  }

  template<typename T>
  static T GetScaleMultiplier(int scale) {
    DCHECK_GE(scale, 0);
    // TODO: this should be a lookup table.
    T result = 1;
    for (int i = 0; i < scale; ++i) {
      result *= 10;
    }
    return result;
  }
};

}

#endif
