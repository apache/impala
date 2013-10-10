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


#ifndef IMPALA_EXPRS_HIVE_UDF_CALL_H
#define IMPALA_EXPRS_HIVE_UDF_CALL_H

#include <string>
#include <boost/scoped_ptr.hpp>

#include "exprs/expr.h"

namespace impala {

class TExprNode;
class RuntimeState;

// Executor for hive udfs using JNI. This works with the UdfExecutor on the
// java side which calls into the actual UDF.
//
// To minimize the JNI overhead, we eliminate as many copies as possible and
// share memory between the native side and java side. Memory in the native heap
// can be read with no issues from java but not vice versa (ptrs in the java heap
// move). Also, JNI calls are cheaper for function calls with no arguments and
// no return value (void).
//
// During Prepare(), we allocate an input buffer that is big enough to store
// all of the inputs (i.e. the slot size). This buffer is passed to the UdfExecutor
// in the constructor. During Evaluate(), the input buffer is populated and
// the UdfExecutor.evaluate() method is called via JNI. For input arguments,
// strings don't need to be treated any differently. The java side can parse
// the ptr and length from the StringValue and then read the ptr directly.
//
// For return values that are fixed size (i.e. not strings), we allocate an
// output buffer in Prepare(). This is also passed to the UdfExecutor in the
// constructor. The UdfExecutor writes to it directly during evaluate().
//
// For strings, we pass a StringValue sized output buffer to the FE. The address
// of the StringValue does not change. When the FE writes the string result, it
// populates the StringValue with the buffer it allocated from its native heap.
// The BE reads the StringValue as normal.
//
// If the UDF ran into an error, the FE throws an exception.
class HiveUdfCall : public Expr {
 public:
  virtual ~HiveUdfCall();
  virtual Status Prepare(RuntimeState* state, const RowDescriptor& row_desc);

 protected:
  friend class Expr;
  friend class StringFunctions;

  HiveUdfCall(const TExprNode& node);
  virtual std::string DebugString() const;

 private:
  static void* Evaluate(Expr* e, TupleRow* row);

  RuntimeState* state_;
  std::string hdfs_location_;
  std::string class_name_;

  struct JniContext;
  boost::scoped_ptr<JniContext> jni_context_;

  // input_byte_offsets_[i] is the byte offset child ith's input argument should
  // be written to.
  std::vector<int> input_byte_offsets_;
};

}

#endif
