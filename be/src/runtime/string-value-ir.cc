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

#include "codegen/impala-ir.h"
#include "runtime/string-value.h"

namespace impala {

IR_ALWAYS_INLINE int StringValue::IrLen() const { return Len(); }

IR_ALWAYS_INLINE char* StringValue::IrPtr() const { return Ptr(); }

IR_ALWAYS_INLINE void StringValue::IrSetLen(int len) { SetLen(len); }

IR_ALWAYS_INLINE void StringValue::IrAssign(char* ptr, int len) {
  Assign(ptr, len);
}

IR_ALWAYS_INLINE void StringValue::IrUnsafeAssign(char* ptr, int len) {
  UnsafeAssign(ptr, len);
}

IR_ALWAYS_INLINE void StringValue::IrClear() { Clear(); }

}
