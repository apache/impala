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

# This script will generate the implementation of the simple functions for the BE.
# These include:
#   - Arithmetic functions
#   - Binary functions
#   - Cast functions
#
# The script outputs (run: 'impala/common/function-registry/gen_functions.py')
#   - header and implemention for above functions:
#     - impala/be/src/generated-sources/opcode/functions.[h/cc]
#   - python file that contains the metadata for those functions:
#     - impala/common/function-registry/generated_functions.py

unary_op = Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op = e->children()[0];\n\
  ${native_type1}* val = reinterpret_cast<${native_type1}*>(op->GetValue(row));\n\
  if (val == NULL) return NULL;\n\
  e->result_.${result_field} = ${native_op} *val;\n\
  return &e->result_.${result_field};\n\
}\n\n")

binary_op = Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op1 = e->children()[0];\n\
  ${native_type1}* val1 = reinterpret_cast<${native_type1}*>(op1->GetValue(row));\n\
  Expr* op2 = e->children()[1];\n\
  ${native_type2}* val2 = reinterpret_cast<${native_type2}*>(op2->GetValue(row));\n\
  if (val1 == NULL || val2 == NULL) return NULL;\n\
  e->result_.${result_field} = (*val1 ${native_op} *val2);\n\
  return &e->result_.${result_field};\n\
}\n\n")

binary_op_check_zero = Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op1 = e->children()[0];\n\
  ${native_type1}* val1 = reinterpret_cast<${native_type1}*>(op1->GetValue(row));\n\
  Expr* op2 = e->children()[1];\n\
  ${native_type2}* val2 = reinterpret_cast<${native_type2}*>(op2->GetValue(row));\n\
  if (val1 == NULL || val2 == NULL || *val2 == 0) return NULL;\n\
  e->result_.${result_field} = (*val1 ${native_op} *val2);\n\
  return &e->result_.${result_field};\n\
}\n\n")

binary_func = Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op1 = e->children()[0];\n\
  ${native_type1}* val1 = reinterpret_cast<${native_type1}*>(op1->GetValue(row));\n\
  Expr* op2 = e->children()[1];\n\
  ${native_type2}* val2 = reinterpret_cast<${native_type2}*>(op2->GetValue(row));\n\
  if (val1 == NULL || val2 == NULL) return NULL;\n\
  e->result_.${result_field} = val1->${native_func}(*val2);\n\
  return &e->result_.${result_field};\n\
}\n\n")

cast = Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op = e->children()[0];\n\
  ${native_type1}* val = reinterpret_cast<${native_type1}*>(op->GetValue(row));\n\
  if (val == NULL) return NULL;\n\
  e->result_.${result_field} = *val;\n\
  return &e->result_.${result_field};\n\
}\n\n")

string_to_int = Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op = e->children()[0];\n\
  ${native_type1}* val = reinterpret_cast<${native_type1}*>(op->GetValue(row));\n\
  if (val == NULL) return NULL;\n\
  StringParser::ParseResult result;\n\
  e->result_.${result_field} = \
      StringParser::StringToInt<${native_type2}>(val->ptr, val->len, &result);\n\
  if (UNLIKELY(result != StringParser::PARSE_SUCCESS)) return NULL;\n\
  return &e->result_.${result_field};\n\
}\n\n")

string_to_float = Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op = e->children()[0];\n\
  ${native_type1}* val = reinterpret_cast<${native_type1}*>(op->GetValue(row));\n\
  if (val == NULL) return NULL;\n\
  StringParser::ParseResult result;\n\
  e->result_.${result_field} = \
      StringParser::StringToFloat<${native_type2}>(val->ptr, val->len, &result);\n\
  if (UNLIKELY(result != StringParser::PARSE_SUCCESS)) return NULL;\n\
  return &e->result_.${result_field};\n\
}\n\n")

string_to_timestamp = Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op = e->children()[0];\n\
  ${native_type1}* val = reinterpret_cast<${native_type1}*>(op->GetValue(row));\n\
  if (val == NULL) return NULL;\n\
  e->result_.${result_field} = TimestampValue(val->ptr, val->len);\n\
  return &e->result_.${result_field};\n\
}\n\n")

numeric_to_string = Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op = e->children()[0];\n\
  ${native_type1}* val = reinterpret_cast<${native_type1}*>(op->GetValue(row));\n\
  if (val == NULL) return NULL;\n\
  e->result_.SetStringVal(lexical_cast<string>(*val));\n\
  return &e->result_.${result_field};\n\
}\n\n")

# Need to special case tinyint.  boost thinks it is a char and handles it differently.
# e.g. '0' is written as an empty string.
string_to_tinyint = Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op = e->children()[0];\n\
  ${native_type1}* val = reinterpret_cast<${native_type1}*>(op->GetValue(row));\n\
  if (val == NULL) return NULL;\n\
  string tmp(val->ptr, val->len);\n\
  try {\n\
    e->result_.${result_field} = static_cast<int8_t>(lexical_cast<int16_t>(tmp));\n\
  } catch (bad_lexical_cast &) {\n\
    return NULL;\n\
  }\n\
  return &e->result_.${result_field};\n\
}\n\n")

tinyint_to_string = Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op = e->children()[0];\n\
  ${native_type1}* val = reinterpret_cast<${native_type1}*>(op->GetValue(row));\n\
  if (val == NULL) return NULL;\n\
  int64_t tmp_val = *val;\n\
  e->result_.SetStringVal(lexical_cast<string>(tmp_val));\n\
  return &e->result_.${result_field};\n\
}\n\n")

case = Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  CaseExpr* expr = static_cast<CaseExpr*>(e);\n\
  int num_children = e->GetNumChildren();\n\
  int loop_end = (expr->has_else_expr()) ? num_children - 1 : num_children;\n\
  // Make sure we set the right compute function.\n\
  DCHECK_EQ(expr->has_case_expr(), true);\n\
  // Need at least case, when and then expr, and optionally an else.\n\
  DCHECK_GE(num_children, (expr->has_else_expr()) ? 4 : 3);\n\
  // All case and when exprs return the same type (we guaranteed that during analysis).\n\
  void* case_val = e->children()[0]->GetValue(row);\n\
  if (case_val == NULL) {\n\
    if (expr->has_else_expr()) {\n\
      // Return else value.\n\
      return e->children()[num_children - 1]->GetValue(row);\n\
    } else {\n\
      return NULL;\n\
    }\n\
  }\n\
  for (int i = 1; i < loop_end; i += 2) {\n\
    ${native_type1}* when_val =\n\
        reinterpret_cast<${native_type1}*>(e->children()[i]->GetValue(row));\n\
    if (when_val == NULL) continue;\n\
    if (*reinterpret_cast<${native_type1}*>(case_val) == *when_val) {\n\
      // Return then value.\n\
      return e->children()[i + 1]->GetValue(row);\n\
    }\n\
  }\n\
  if (expr->has_else_expr()) {\n\
    // Return else value.\n\
    return e->children()[num_children - 1]->GetValue(row);\n\
  }\n\
  return NULL;\n\
}\n\n")

python_template = Template("\
  ['${fn_name}', '${return_type}', [${args}], 'ComputeFunctions::${fn_signature}', []], \n")

# Mapping of function to template
templates = {
  'Add'         : binary_op,
  'Subtract'    : binary_op,
  'Multiply'    : binary_op,
  'Divide'      : binary_op,
  'Int_Divide'  : binary_op_check_zero,
  'Mod'         : binary_op_check_zero,
  'BitAnd'      : binary_op,
  'BitXor'      : binary_op,
  'BitOr'       : binary_op,
  'BitNot'      : unary_op,
  'Eq'          : binary_op,
  'Ne'          : binary_op,
  'Ge'          : binary_op,
  'Gt'          : binary_op,
  'Lt'          : binary_op,
  'Le'          : binary_op,
  'Cast'        : cast,
}

# Some aggregate types that are useful for defining functions
types = {
  'BOOLEAN'       : ['BOOLEAN'],
  'TINYINT'       : ['TINYINT'],
  'SMALLINT'      : ['SMALLINT'],
  'INT'           : ['INT'],
  'BIGINT'        : ['BIGINT'],
  'FLOAT'         : ['FLOAT'],
  'DOUBLE'        : ['DOUBLE'],
  'STRING'        : ['STRING'],
  'TIMESTAMP'     : ['TIMESTAMP'],
  'INT_TYPES'     : ['TINYINT', 'SMALLINT', 'INT', 'BIGINT'],
  'FLOAT_TYPES'   : ['FLOAT', 'DOUBLE'],
  'NUMERIC_TYPES' : ['TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'FLOAT', 'DOUBLE'],
  'NATIVE_TYPES'  : ['BOOLEAN', 'TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'FLOAT', 'DOUBLE'],
  'STRCAST_TYPES' : ['BOOLEAN', 'SMALLINT', 'INT', 'BIGINT', 'FLOAT', 'DOUBLE'],
  'ALL_TYPES'     : ['BOOLEAN', 'TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'FLOAT',\
                     'DOUBLE', 'STRING', 'TIMESTAMP'],
  'MAX_TYPES'     : ['BIGINT', 'DOUBLE'],
}

# Operation, [ReturnType], [[Args1], [Args2], ... [ArgsN]]
functions = [
  # Arithmetic Expr
  ['Add', ['MAX_TYPES'], [['MAX_TYPES'], ['MAX_TYPES']] ],
  ['Subtract', ['MAX_TYPES'], [['MAX_TYPES'], ['MAX_TYPES']] ],
  ['Multiply', ['MAX_TYPES'], [['MAX_TYPES'], ['MAX_TYPES']] ],
  ['Divide', ['DOUBLE'], [['DOUBLE'], ['DOUBLE']] ],
  ['Int_Divide', ['INT_TYPES'], [['INT_TYPES'], ['INT_TYPES']] ],
  ['Mod', ['INT_TYPES'], [['INT_TYPES'], ['INT_TYPES']] ],
  ['BitAnd', ['INT_TYPES'], [['INT_TYPES'], ['INT_TYPES']] ],
  ['BitXor', ['INT_TYPES'], [['INT_TYPES'], ['INT_TYPES']] ],
  ['BitOr', ['INT_TYPES'], [['INT_TYPES'], ['INT_TYPES']] ],
  ['BitNot', ['INT_TYPES'], [['INT_TYPES']] ],

  # BinaryPredicates
  ['Eq', ['BOOLEAN'], [['NATIVE_TYPES'], ['NATIVE_TYPES']] ],
  ['Ne', ['BOOLEAN'], [['NATIVE_TYPES'], ['NATIVE_TYPES']] ],
  ['Gt', ['BOOLEAN'], [['NATIVE_TYPES'], ['NATIVE_TYPES']] ],
  ['Lt', ['BOOLEAN'], [['NATIVE_TYPES'], ['NATIVE_TYPES']] ],
  ['Ge', ['BOOLEAN'], [['NATIVE_TYPES'], ['NATIVE_TYPES']] ],
  ['Le', ['BOOLEAN'], [['NATIVE_TYPES'], ['NATIVE_TYPES']] ],
  ['Eq', ['BOOLEAN'], [['STRING'], ['STRING']], binary_func ],
  ['Ne', ['BOOLEAN'], [['STRING'], ['STRING']], binary_func ],
  ['Gt', ['BOOLEAN'], [['STRING'], ['STRING']], binary_func ],
  ['Lt', ['BOOLEAN'], [['STRING'], ['STRING']], binary_func ],
  ['Ge', ['BOOLEAN'], [['STRING'], ['STRING']], binary_func ],
  ['Le', ['BOOLEAN'], [['STRING'], ['STRING']], binary_func ],
  ['Eq', ['BOOLEAN'], [['TIMESTAMP'], ['TIMESTAMP']], ],
  ['Ne', ['BOOLEAN'], [['TIMESTAMP'], ['TIMESTAMP']], ],
  ['Gt', ['BOOLEAN'], [['TIMESTAMP'], ['TIMESTAMP']], ],
  ['Lt', ['BOOLEAN'], [['TIMESTAMP'], ['TIMESTAMP']], ],
  ['Ge', ['BOOLEAN'], [['TIMESTAMP'], ['TIMESTAMP']], ],
  ['Le', ['BOOLEAN'], [['TIMESTAMP'], ['TIMESTAMP']], ],

  # Casts
  ['Cast', ['BOOLEAN'], [['NATIVE_TYPES'], ['BOOLEAN']] ],
  ['Cast', ['TINYINT'], [['NATIVE_TYPES'], ['TINYINT']] ],
  ['Cast', ['SMALLINT'], [['NATIVE_TYPES'], ['SMALLINT']] ],
  ['Cast', ['INT'], [['NATIVE_TYPES'], ['INT']] ],
  ['Cast', ['BIGINT'], [['NATIVE_TYPES'], ['BIGINT']] ],
  ['Cast', ['FLOAT'], [['NATIVE_TYPES'], ['FLOAT']] ],
  ['Cast', ['DOUBLE'], [['NATIVE_TYPES'], ['DOUBLE']] ],
  ['Cast', ['INT_TYPES'], [['STRING'], ['INT_TYPES']], string_to_int ],
  ['Cast', ['FLOAT_TYPES'], [['STRING'], ['FLOAT_TYPES']], string_to_float ],
  ['Cast', ['STRING'], [['STRCAST_TYPES'], ['STRING']], numeric_to_string ],
  ['Cast', ['STRING'], [['TINYINT'], ['STRING']], tinyint_to_string ],
  ['Cast', ['NATIVE_TYPES'], [['TIMESTAMP'], ['NATIVE_TYPES']]],
  ['Cast', ['STRING'], [['TIMESTAMP'], ['STRING']], numeric_to_string ],
  ['Cast', ['TIMESTAMP'], [['STRING'], ['TIMESTAMP']], string_to_timestamp],
  ['Cast', ['TIMESTAMP'], [['NATIVE_TYPES'], ['TIMESTAMP']], ],

  # Case
  # The case expr is special because it has a variable number of function args,
  # but we guarantee that all of them are of the same type during query analysis,
  # so we just list exactly one here.
  # In addition, the return type given here is a dummy, because it is
  # not necessarily the same as the function args type.
  ['Case', ['ALL_TYPES'], [['ALL_TYPES']], case],
]

native_types = {
  'BOOLEAN'       : 'bool',
  'TINYINT'       : 'char',
  'SMALLINT'      : 'short',
  'INT'           : 'int',
  'BIGINT'        : 'long',
  'FLOAT'         : 'float',
  'DOUBLE'        : 'double',
  'STRING'        : 'StringValue',
  'TIMESTAMP'     : 'TimestampValue',
}

# Portable type used in the function implementation
implemented_types = {
  'BOOLEAN'       : 'bool',
  'TINYINT'       : 'int8_t',
  'SMALLINT'      : 'int16_t',
  'INT'           : 'int32_t',
  'BIGINT'        : 'int64_t',
  'FLOAT'         : 'float',
  'DOUBLE'        : 'double',
  'STRING'        : 'StringValue',
  'TIMESTAMP'     : 'TimestampValue',
}
result_fields = {
  'BOOLEAN'       : 'bool_val',
  'TINYINT'       : 'tinyint_val',
  'SMALLINT'      : 'smallint_val',
  'INT'           : 'int_val',
  'BIGINT'        : 'bigint_val',
  'FLOAT'         : 'float_val',
  'DOUBLE'        : 'double_val',
  'STRING'        : 'string_val',
  'TIMESTAMP'     : 'timestamp_val',
}

native_ops = {
  'BITAND'     : '&',
  'BITNOT'     : '~',
  'BITOR'      : '|',
  'BITXOR'     : '^',
  'DIVIDE'     : '/',
  'EQ'         : '==',
  'GT'         : '>',
  'GE'         : '>=',
  'INT_DIVIDE' : '/',
  'SUBTRACT'   : '-',
  'MOD'        : '%',
  'MULTIPLY'   : '*',
  'LT'         : '<',
  'LE'         : '<=',
  'NE'         : '!=',
  'ADD'        : '+',
}

native_funcs = {
  'EQ' : 'Eq',
  'LE' : 'Le',
  'LT' : 'Lt',
  'NE' : 'Ne',
  'GE' : 'Ge',
  'GT' : 'Gt',
}

cc_preamble = '\
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
// This is a generated file, DO NOT EDIT.\n\
// To add new functions, see impala/common/function-registry/gen_opcodes.py\n\
\n\
#include "opcode/functions.h"\n\
#include "exprs/expr.h"\n\
#include "exprs/case-expr.h"\n\
#include "runtime/string-value.inline.h"\n\
#include "runtime/tuple-row.h"\n\
#include "util/string-parser.h"\n\
#include <boost/lexical_cast.hpp>\n\
\n\
using namespace boost;\n\
using namespace std;\n\
\n\
namespace impala { \n\
\n'

cc_epilogue = '\
}\n'

h_preamble = '\
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
// To add new functions, see impala/common/function-registry/gen_opcodes.py\n\
\n\
#ifndef IMPALA_OPCODE_FUNCTIONS_H\n\
#define IMPALA_OPCODE_FUNCTIONS_H\n\
\n\
namespace impala {\n\
class Expr;\n\
class OpcodeRegistry;\n\
class TupleRow;\n\
\n\
class ComputeFunctions {\n\
 public:\n'

h_epilogue = '\
};\n\
\n\
}\n\
\n\
#endif\n'

python_preamble = '\
#!/usr/bin/env python\n\
# Copyright 2012 Cloudera Inc.\n\
#\n\
# Licensed under the Apache License, Version 2.0 (the "License");\n\
# you may not use this file except in compliance with the License.\n\
# You may obtain a copy of the License at\n\
#\n\
# http://www.apache.org/licenses/LICENSE-2.0\n\
#\n\
# Unless required by applicable law or agreed to in writing, software\n\
# distributed under the License is distributed on an "AS IS" BASIS,\n\
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.\n\
# See the License for the specific language governing permissions and\n\
# limitations under the License.\n\
\n\
# This is a generated file, DO NOT EDIT IT.\n\
# To add new functions, see impala/common/function-registry/gen_opcodes.py\n\
\n\
functions = [\n'

python_epilogue = ']'

header_template = Template("\
  static void* ${fn_signature}(Expr* e, TupleRow* row);\n")

BE_PATH = "../../be/generated-sources/opcode/"
if not os.path.exists(BE_PATH):
  os.makedirs(BE_PATH)

# Expand the signature data for template substitution.  Returns
# a dictionary with all the entries for all the templates used in this script
def initialize_sub(op, return_type, arg_types):
  sub = {}
  sub["fn_name"] = op
  sub["fn_signature"] = op
  sub["return_type"] = return_type
  sub["result_field"] = result_fields[return_type]
  sub["args"] = ""
  if op.upper() in native_ops:
    sub["native_op"] = native_ops[op.upper()]
  for idx in range(0, len(arg_types)):
    arg = arg_types[idx]
    sub["fn_signature"] += "_" + native_types[arg]
    sub["native_type" + repr(idx + 1)] = implemented_types[arg]
    sub["args"] += "'" + arg + "', "
  return sub

if __name__ == "__main__":
  h_file = open(BE_PATH + 'functions.h', 'w')
  cc_file = open(BE_PATH + 'functions.cc', 'w')
  python_file = open('generated_functions.py', 'w')
  h_file.write(h_preamble)
  cc_file.write(cc_preamble)
  python_file.write(python_preamble)

  # Generate functions and headers
  for func_data in functions:

    op = func_data[0]
    # If a specific template has been specified, use that one.
    if len(func_data) >= 4 :
      template = func_data[3]
    else :
      # Skip functions with no template (shouldn't be auto-generated)
      if not op in templates:
        continue
      template = templates[op]

    # Expand all arguments
    return_types = []
    for ret in func_data[1]:
      for t in types[ret]:
        return_types.append(t)
    signatures = []
    for args in func_data[2]:
      expanded_arg = []
      for arg in args:
        for t in types[arg]:
          expanded_arg.append(t)
      signatures.append(expanded_arg)

    # Put arguments into substitution structure
    num_functions = 0
    for args in signatures:
      num_functions = max(num_functions, len(args))
    num_functions = max(num_functions, len(return_types))
    num_args = len(signatures)

    # Validate the input is correct
    if len(return_types) != 1 and len(return_types) != num_functions:
      print "Invalid Declaration: " + func_data
      sys.exit(1)

    for args in signatures:
      if len(args) != 1 and len(args) != num_functions:
        print "Invalid Declaration: " + func_data
        sys.exit(1)

    # Iterate over every function signature to generate
    for i in range(0, num_functions):
      if len(return_types) == 1:
        return_type = return_types[0]
      else:
        return_type = return_types[i]

      arg_types = []
      for j in range(0, num_args):
        if len(signatures[j]) == 1:
          arg_types.append(signatures[j][0])
        else:
          arg_types.append(signatures[j][i])

      # At this point, 'return_type' is a single type and 'arg_types'
      # is a list of single types
      sub = initialize_sub(op, return_type, arg_types)
      if template == binary_func :
        sub["native_func"] = native_funcs[op.upper()]

      h_file.write(header_template.substitute(sub))
      cc_file.write(template.substitute(sub))
      python_file.write(python_template.substitute(sub))

  h_file.write(h_epilogue)
  cc_file.write(cc_epilogue)
  python_file.write(python_epilogue)
  h_file.close()
  cc_file.close()
  python_file.close()
