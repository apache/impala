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


#ifndef IMPALA_EXPRS_UTILITY_FUNCTIONS_H
#define IMPALA_EXPRS_UTILITY_FUNCTIONS_H

namespace impala {

class Expr;
class OpcodeRegistry;
class TupleRow;

class UtilityFunctions {
 public:
  // Implementations of the FnvHash function. Returns the Fowler-Noll-Vo hash of the
  // input as an int64_t.
  template <int BYTE_SIZE>
  static void* FnvHash(Expr* e, TupleRow* row);
  static void* FnvHashString(Expr* e, TupleRow* row);

  // Implementation of the user() function. Returns the username of the user who executed
  // this function.
  static void* User(Expr* e, TupleRow* row);

  // Implementation of the version() function. Returns the version string.
  static void* Version(Expr* e, TupleRow* row);

  // Testing function that sleeps for the specified number of milliseconds. Returns true.
  static void* Sleep(Expr* e, TupleRow* row);
};

}

#endif
