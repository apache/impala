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


#ifndef IMPALA_BIT_UTIL_H
#define IMPALA_BIT_UTIL_H

namespace impala {

// Utility class to do standard bit tricks
// TODO: is this in boost or something else like that?
class BitUtil {
 public:
  // Returns the ceil of value/divisor
  static inline int Ceil(int value, int divisor) {
    return value / divisor + (value % divisor != 0);
  }
};

}

#endif
