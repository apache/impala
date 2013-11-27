#!/usr/bin/env python
# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from string import Template
import os

# This script will generate two headers that describe all of the clang cross compiled
# functions.
# The script outputs (run: 'impala/common/function-registry/gen_functions.py')
#   - be/src/generated-sources/impala-ir/impala-ir-functions.h
#     This file contains enums for all of the cross compiled functions
#   - be/src/generated-sources/impala-ir/impala-ir-function-names.h
#     This file contains a mapping of <string, enum>

# Mapping of enum to compiled function name.  The compiled function name only has to
# be a substring of the actual, mangled compiler generated name.
# TODO: should we work out the mangling rules?
ir_functions = [
  ["AGG_NODE_PROCESS_ROW_BATCH_WITH_GROUPING", "ProcessRowBatchWithGrouping"],
  ["AGG_NODE_PROCESS_ROW_BATCH_NO_GROUPING", "ProcessRowBatchNoGrouping"],
  ["EXPR_GET_VALUE", "IrExprGetValue"],
  ["HASH_CRC", "IrCrcHash"],
  ["HASH_FNV", "IrFnvHash"],
  ["HASH_JOIN_PROCESS_BUILD_BATCH", "ProcessBuildBatch"],
  ["HASH_JOIN_PROCESS_PROBE_BATCH", "ProcessProbeBatch"],
  ["DECODE_AVRO_DATA", "DecodeAvroData"],
  ["READ_UNION_TYPE", "ReadUnionType"],
  ["READ_AVRO_BOOLEAN", "ReadAvroBoolean"],
  ["READ_AVRO_INT32", "ReadAvroInt32"],
  ["READ_AVRO_INT64", "ReadAvroInt64"],
  ["READ_AVRO_FLOAT", "ReadAvroFloat"],
  ["READ_AVRO_DOUBLE", "ReadAvroDouble"],
  ["READ_AVRO_STRING", "ReadAvroString"],
  ["HDFS_SCANNER_WRITE_ALIGNED_TUPLES", "WriteAlignedTuples"],
  ["STRING_VALUE_EQ", "StringValueEQ"],
  ["STRING_VALUE_NE", "StringValueNE"],
  ["STRING_VALUE_GE", "StringValueGE"],
  ["STRING_VALUE_GT", "StringValueGT"],
  ["STRING_VALUE_LT", "StringValueLT"],
  ["STRING_VALUE_LE", "StringValueLE"],
  ["STRING_TO_BOOL", "IrStringToBool"],
  ["STRING_TO_INT8", "IrStringToInt8"],
  ["STRING_TO_INT16", "IrStringToInt16"],
  ["STRING_TO_INT32", "IrStringToInt32"],
  ["STRING_TO_INT64", "IrStringToInt64"],
  ["STRING_TO_FLOAT", "IrStringToFloat"],
  ["STRING_TO_DOUBLE", "IrStringToDouble"],  
  ["IS_NULL_STRING", "IrIsNullString"],
  ["GENERIC_IS_NULL_STRING", "IrGenericIsNullString"],
]

enums_preamble = '\
// Copyright 2012 Cloudera Inc.\n\
//\n\
// Licensed under the Apache License, Version 2.0 (the "License");\n\
// you may not use this file except in compliance with the License.\n\
// You may obtain a copy of the License at\n\
//\n\
// http://www.apache.org/licenses/LICENSE-2.0\n\
//\n\
// Unless required by applicable law or agreed to in writing, software\n\
// distributed under the License is distributed on an "AS IS" BASIS,\n\
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n\
// See the License for the specific language governing permissions and\n\
// limitations under the License.\n\
\n\
// This is a generated file, DO NOT EDIT IT.\n\
// To add new functions, see be/src/codegen/gen_ir_descriptions.py.\n\
\n\
#ifndef IMPALA_IR_FUNCTIONS_H\n\
#define IMPALA_IR_FUNCTIONS_H\n\
\n\
namespace impala {\n\
\n\
class IRFunction {\n\
 public:\n\
  enum Type {\n'

enums_epilogue = '\
  };\n\
};\n\
\n\
}\n\
\n\
#endif\n'

names_preamble = '\
// Copyright 2012 Cloudera Inc.\n\
//\n\
// Licensed under the Apache License, Version 2.0 (the "License");\n\
// you may not use this file except in compliance with the License.\n\
// You may obtain a copy of the License at\n\
//\n\
// http://www.apache.org/licenses/LICENSE-2.0\n\
//\n\
// Unless required by applicable law or agreed to in writing, software\n\
// distributed under the License is distributed on an "AS IS" BASIS,\n\
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n\
// See the License for the specific language governing permissions and\n\
// limitations under the License.\n\
\n\
// This is a generated file, DO NOT EDIT IT.\n\
// To add new functions, see be/src/codegen/gen_ir_descriptions.py.\n\
\n\
#ifndef IMPALA_IR_FUNCTION_NAMES_H\n\
#define IMPALA_IR_FUNCTION_NAMES_H\n\
\n\
#include "impala-ir/impala-ir-functions.h"\n\
\n\
namespace impala {\n\
\n\
static struct {\n\
  std::string fn_name; \n\
  IRFunction::Type fn; \n\
} FN_MAPPINGS[] = {\n'

names_epilogue = '\
};\n\
\n\
}\n\
\n\
#endif\n'

BE_PATH = os.environ['IMPALA_HOME'] + "/be/generated-sources/impala-ir/"
if not os.path.exists(BE_PATH):
  os.makedirs(BE_PATH)

if __name__ == "__main__":
  print "Generating IR description files"
  enums_file = open(BE_PATH + 'impala-ir-functions.h', 'w')
  enums_file.write(enums_preamble)

  names_file = open(BE_PATH + 'impala-ir-names.h', 'w')
  names_file.write(names_preamble);

  idx = 0;
  enums_file.write("    FN_START = " + str(idx) + ",\n")
  for fn in ir_functions:
    enum = fn[0]
    fn_name = fn[1]
    enums_file.write("    " + enum + " = " + str(idx) + ",\n")
    names_file.write("  { \"" + fn_name + "\", IRFunction::" + enum + " },\n")
    idx = idx + 1;
  enums_file.write("    FN_END = " + str(idx) + "\n")


  enums_file.write(enums_epilogue)
  enums_file.close()

  names_file.write(names_epilogue)
  names_file.close()
