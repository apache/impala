// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include <boost/static_assert.hpp>

#include "common/hdfs.h"
#include "runtime/string-value.h"
#include "runtime/timestamp-value.h"

namespace impala {
// This class is unused.  It contains static (compile time) asserts.
// This is useful to validate struct sizes and other similar things
// at compile time.  If these asserts fail, the compile will fail.
class UnusedClass {
 private:
  BOOST_STATIC_ASSERT(sizeof(StringValue) == 16);
  BOOST_STATIC_ASSERT(offsetof(StringValue, len) == 8);
  BOOST_STATIC_ASSERT(sizeof(TimestampValue) == 16);
  BOOST_STATIC_ASSERT(offsetof(TimestampValue, date_) == 8);
  BOOST_STATIC_ASSERT(sizeof(hdfsFS) == sizeof(void*));
  BOOST_STATIC_ASSERT(sizeof(hdfsFile) == sizeof(void*));
};

}
