// Copyright (c) 2012 Cloudera, Inc. All rights reserved.

// Includes source files for cross compiling to IR.  By having all of the code in
// one .cc file, clang will be able to compile all of code into one module.
// All cross compiled code needs to be added to this file.
// All files here must be added explicitly to the codegen/CMakeLists.txt dependency list

#ifdef IR_COMPILE
#include "exec/aggregation-node-ir.cc"
#include "exec/hash-join-node-ir.cc"
#include "exec/hdfs-scanner-ir.cc"
#include "runtime/string-value-ir.cc"
#include "util/hash-util-ir.cc"
#else
#error "This file should only be used for cross compiling to IR."
#endif

