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

#include <boost/static_assert.hpp>

#include "common/hdfs.h"
#include "runtime/collection-value.h"
#include "runtime/date-value.h"
#include "runtime/string-value.h"
#include "runtime/timestamp-value.h"
#include "udf/udf.h"

namespace impala {
// This class is unused.  It contains static (compile time) asserts.
// This is useful to validate struct sizes and other similar things
// at compile time.  If these asserts fail, the compile will fail.
class UnusedClass {
 private:
  BOOST_STATIC_ASSERT(sizeof(StringValue) == 12);
  BOOST_STATIC_ASSERT(sizeof(TimestampValue) == 16);
  BOOST_STATIC_ASSERT(offsetof(TimestampValue, date_) == 8);
  BOOST_STATIC_ASSERT(sizeof(boost::posix_time::time_duration) == 8);
  BOOST_STATIC_ASSERT(sizeof(boost::gregorian::date) == 4);
  BOOST_STATIC_ASSERT(sizeof(CollectionValue) == 12);
  BOOST_STATIC_ASSERT(sizeof(hdfsFS) == sizeof(void*));
  BOOST_STATIC_ASSERT(sizeof(hdfsFile) == sizeof(void*));
  BOOST_STATIC_ASSERT(sizeof(DateValue) == 4);

  // If the memory layout of any of these types changes, it will be necessary to change
  // LlvmCodeGen::GetUdfValType(), and we may also run into calling convention problems
  // with codegen'd calls to UDFs.
  BOOST_STATIC_ASSERT(sizeof(impala_udf::BooleanVal) == 2);
  BOOST_STATIC_ASSERT(sizeof(impala_udf::TinyIntVal) == 2);
  BOOST_STATIC_ASSERT(sizeof(impala_udf::SmallIntVal) == 4);
  BOOST_STATIC_ASSERT(sizeof(impala_udf::IntVal) == 8);
  BOOST_STATIC_ASSERT(sizeof(impala_udf::BigIntVal) == 16);
  BOOST_STATIC_ASSERT(sizeof(impala_udf::FloatVal) == 8);
  BOOST_STATIC_ASSERT(sizeof(impala_udf::DoubleVal) == 16);
  BOOST_STATIC_ASSERT(sizeof(impala_udf::StringVal) == 16);
  BOOST_STATIC_ASSERT(sizeof(impala_udf::DateVal) == 8);
};

}
