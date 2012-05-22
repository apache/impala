// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

#ifdef IR_COMPILE
#include "runtime/string-value.h"

using namespace impala;

extern "C" 
bool StringValueEQ(const StringValue* s1, const StringValue* s2) {
  return s1->Eq(*s2);
}

extern "C"
bool StringValueNE(const StringValue* s1, const StringValue* s2) {
  return s1->Ne(*s2);
}

extern "C"
bool StringValueLT(const StringValue* s1, const StringValue* s2) {
  return s1->Lt(*s2);
}

extern "C"
bool StringValueLE(const StringValue* s1, const StringValue* s2) {
  return s1->Le(*s2);
}

extern "C"
bool StringValueGT(const StringValue* s1, const StringValue* s2) {
  return s1->Gt(*s2);
}

extern "C"
bool StringValueGE(const StringValue* s1, const StringValue* s2) {
  return s1->Ge(*s2);
}
#else
#error "This file should only be used for cross compiling to IR."
#endif

