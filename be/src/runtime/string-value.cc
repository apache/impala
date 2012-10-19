// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#include "runtime/string-value.h"
#include <cstring>

using namespace std;

namespace impala {

const char* StringValue::LLVM_CLASS_NAME = "struct.impala::StringValue";

string StringValue::DebugString() const {
  return string(ptr, len);
}

ostream& operator<<(ostream& os, const StringValue& string_value) {
  return os << string_value.DebugString();
}


}
