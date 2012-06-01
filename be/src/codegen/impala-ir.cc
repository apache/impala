// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

// Includes source files for cross compiling to IR.  By having all of the code in
// one .cc file, clang will be able to compile all of code into one module.
// All cross compiled code needs to be added to this file.

#ifdef IR_COMPILE
#include "exec/aggregation-node-ir.cc"
#include "exec/hash-join-node-ir.cc"
#include "exec/hdfs-text-scanner-ir.cc"
#include "runtime/string-value-ir.cc"
#else
#error "This file should only be used for cross compiling to IR."
#endif

