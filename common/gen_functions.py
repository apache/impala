#!/usr/bin/env python

from string import Template
import os

# This script will generate the implementation of the simple functions for the BE.
# These include:
#   - Arithmetic functions
#   - Binary functions
#   - Cast functions
#
# The script outputs (run: 'impala/common/gen_functions.py')
#   - header and implemention for above functions: 
#     - impala/be/src/generated-sources/opcode/functions.[h/cc]
#   - python file that contains the metadata for theose functions: 
#     - impala/common/generated_functions.py

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
  'INT_TYPES'     : ['TINYINT', 'SMALLINT', 'INT', 'BIGINT'],
  'NUMERIC_TYPES' : ['TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'FLOAT', 'DOUBLE'],
  'NATIVE_TYPES'  : ['BOOLEAN', 'TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'FLOAT', 'DOUBLE'],
  'ALL_TYPES'     : ['BOOLEAN', 'TINYINT', 'SMALLINT', 'INT', 'BIGINT', 'FLOAT', 'DOUBLE', 'STRING'],
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
  ['Eq', ['BOOLEAN'], [['STRING'], ['STRING']] ], 
  ['Ne', ['BOOLEAN'], [['STRING'], ['STRING']] ], 
  ['Gt', ['BOOLEAN'], [['STRING'], ['STRING']] ], 
  ['Lt', ['BOOLEAN'], [['STRING'], ['STRING']] ], 
  ['Ge', ['BOOLEAN'], [['STRING'], ['STRING']] ], 
  ['Le', ['BOOLEAN'], [['STRING'], ['STRING']] ], 

  # Casts
  ['Cast', ['BOOLEAN'], [['NATIVE_TYPES'], ['BOOLEAN']] ],
  ['Cast', ['TINYINT'], [['NATIVE_TYPES'], ['TINYINT']] ],
  ['Cast', ['SMALLINT'], [['NATIVE_TYPES'], ['SMALLINT']] ],
  ['Cast', ['INT'], [['NATIVE_TYPES'], ['INT']] ],
  ['Cast', ['BIGINT'], [['NATIVE_TYPES'], ['BIGINT']] ],
  ['Cast', ['FLOAT'], [['NATIVE_TYPES'], ['FLOAT']] ],
  ['Cast', ['DOUBLE'], [['NATIVE_TYPES'], ['DOUBLE']] ],
  ['Cast', ['NATIVE_TYPES'], [['STRING'], ['NATIVE_TYPES']] ],
  ['Cast', ['STRING'], [['NATIVE_TYPES'], ['STRING']] ],
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
// Copyright (c) 2011 Cloudera, Inc. All rights reserved.\n\
// This is a generated file, DO NOT EDIT.\n\
// To add new functions, see impala/common/gen_opcodes.py\n\
\n\
#include "opcode/functions.h"\n\
#include "exprs/expr.h"\n\
#include "runtime/tuple-row.h"\n\
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
// Copyright (c) 2011 Cloudera, Inc. All rights reserved.\n\
// This is a generated file, DO NOT EDIT IT.\n\
// To add new functions, see impala/common/gen_opcodes.py\n\
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
# Copyright (c) 2011 Cloudera, Inc. All rights reserved.\n\
# This is a generated file, DO NOT EDIT IT.\n\
# To add new functions, see impala/common/gen_opcodes.py\n\
\n\
functions = [\n'

python_epilogue = ']'

header_template = Template("\
  static void* ${fn_signature}(Expr* e, TupleRow* row);\n")

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

string_to_numeric = Template("\
void* ComputeFunctions::${fn_signature}(Expr* e, TupleRow* row) {\n\
  Expr* op = e->children()[0];\n\
  ${native_type1}* val = reinterpret_cast<${native_type1}*>(op->GetValue(row));\n\
  if (val == NULL) return NULL;\n\
  string tmp(val->ptr, val->len);\n\
  try {\n\
    e->result_.${result_field} = lexical_cast<${native_type2}>(tmp);\n\
  } catch (bad_lexical_cast &) {\n\
    return NULL;\n\
  }\n\
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

python_template = Template("\
  ['${fn_name}', '${return_type}', [${args}], 'ComputeFunctions::${fn_signature}', []], \n")

# Mapping of function to template
templates = {
  'Add'         : binary_op,
  'Subtract'    : binary_op,
  'Multiply'    : binary_op,
  'Divide'      : binary_op,
  'Int_Divide'  : binary_op,
  'Mod'         : binary_op,
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

BE_PATH = "../be/generated-sources/opcode/"
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
    sub["native_type" + repr(idx + 1)] = native_types[arg]
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
    
    # Skip functions with no template (shouldn't be auto-generated)
    if not func_data[0] in templates:
      continue

    # Expand all arguments
    op = func_data[0]
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
      
      # At this point, 'return_type' is a single type and 'arg_types' is a list of single types
      sub = initialize_sub(op, return_type, arg_types)
      template = templates[op]

      # Code-gen for the bodies requires a bit more information
      if (op == 'Eq' or op == 'Ne' or 
          op == 'Gt' or op == 'Lt' or 
          op == 'Ge' or op == 'Le') and arg_types[0] == 'STRING':
          template = binary_func
          sub["native_func"] = native_funcs[op.upper()]

      if op == 'Cast' and return_type == 'STRING':
        template = numeric_to_string

      if op == 'Cast' and arg_types[0] == 'STRING':
        template = string_to_numeric

      h_file.write(header_template.substitute(sub))
      cc_file.write(template.substitute(sub))
      python_file.write(python_template.substitute(sub))

  h_file.write(h_epilogue)
  cc_file.write(cc_epilogue)
  python_file.write(python_epilogue)
  h_file.close()
  cc_file.close()
  python_file.close()

