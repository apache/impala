// Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#ifndef IMPALA_SERVICE_FE_SUPPORT_H
#define IMPALA_SERVICE_FE_SUPPORT_H

#include "util/jni-util.h"

namespace impala {

// InitFeSupport registers native functions with JNI. When the java
// function FeSupport.EvalPredicate is called within Impalad, the native
// implementation FeSupport_EvalPredicateImpl already exists in Impalad binary.
// In order to expose JNI functions from Impalad binary, we need to "register
// native functions". See this link:
//     http://java.sun.com/docs/books/jni/html/other.html#29535
// for details.
void InitFeSupport();

}
#endif
