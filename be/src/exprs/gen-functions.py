#!/usr/bin/env python

from string import Template

# operators/functions and their names
operators = {
    'add': '+',
    'subtract': '-',
    'multiply': '*',
    'divide': '/',
    'mod': '%',
    'bitand': '&',
    'bitor': '|',
    'bitxor': '^',
    'bitnot': '~',
    'eq': '==',
    'ne': '!=',
    'le': '<=',
    'ge': '>=',
    'lt': '<',
    'gt': '>',
    'eq_fn': 'Eq',
    'ne_fn': 'Ne',
    'le_fn': 'Le',
    'ge_fn': 'Ge',
    'lt_fn': 'Lt',
    'gt_fn': 'Gt',
}

# map of signatures (operand types and return type/result field)
op_signatures = {
    'tinyint_op': ('char', 'tinyint_val'),
    'smallint_op': ('short', 'smallint_val'),
    'int_op': ('int', 'int_val'),
    'bigint_op': ('long', 'bigint_val'),
    'float_op': ('float', 'float_val'),
    'double_op': ('double', 'double_val'),
    'string_op': ('string', 'string_val'),
    'tinyint_pred': ('char', 'bool_val'),
    'smallint_pred': ('short', 'bool_val'),
    'int_pred': ('int', 'bool_val'),
    'bigint_pred': ('long', 'bool_val'),
    'float_pred': ('float', 'bool_val'),
    'double_pred': ('double', 'bool_val'),
    'string_pred': ('StringValue', 'bool_val'),
}

# map from native type to corresponding result field
result_fields = {
    'bool': 'bool_val',
    'char': 'tinyint_val',
    'short': 'smallint_val',
    'int': 'int_val',
    'long': 'bigint_val',
    'float': 'float_val',
    'double': 'double_val',
    'StringValue': 'string_val'
}

binary_op_invocations = [
    ('ArithmeticExpr',
      ['add', 'subtract', 'multiply'],
      ['tinyint_op', 'smallint_op', 'int_op', 'bigint_op', 'float_op', 'double_op']),
    ('ArithmeticExpr',
      ['divide'],
      ['double_op']),
    ('ArithmeticExpr',
      ['mod', 'divide', 'bitand', 'bitor', 'bitxor'],
      ['tinyint_op', 'smallint_op', 'int_op', 'bigint_op']),
    ('BinaryPredicate',
      ['eq', 'ne', 'le', 'ge', 'lt', 'gt'],
      ['tinyint_pred', 'smallint_pred', 'int_pred', 'bigint_pred', 'float_pred', 'double_pred']),
]

member_fn_invocations = [
    ('BinaryPredicate',
      ['eq_fn', 'ne_fn', 'le_fn', 'ge_fn', 'lt_fn', 'gt_fn'],
      ['string_pred']),
]

unary_op_invocations = [
    ('ArithmeticExpr',
      ['bitnot'],
      ['tinyint_op', 'smallint_op', 'int_op', 'bigint_op']),
]

binary_op_template = Template("\
void* GetValueFunctions::${function_name}(Expr* e, TupleRow* row) {\n\
  ${expr_class}* expr = static_cast<${expr_class}*>(e);\n\
  // assert(p->children_.size() == 2);\n\
  Expr* op1 = e->children()[0];\n\
  ${native_type}* val1 = reinterpret_cast<${native_type}*>(op1->GetValue(row));\n\
  Expr* op2 = e->children()[1];\n\
  ${native_type}* val2 = reinterpret_cast<${native_type}*>(op2->GetValue(row));\n\
  if (val1 == NULL || val2 == NULL) return NULL;\n\
  expr->result_.${result_field} = *val1 ${op} *val2;\n\
  return &expr->result_.${result_field};\n\
}\n")

member_fn_template = Template("\
void* GetValueFunctions::${function_name}(Expr* e, TupleRow* row) {\n\
  ${expr_class}* expr = static_cast<${expr_class}*>(e);\n\
  // assert(p->children_.size() == 2);\n\
  Expr* op1 = e->children()[0];\n\
  ${native_type}* val1 = reinterpret_cast<${native_type}*>(op1->GetValue(row));\n\
  Expr* op2 = e->children()[1];\n\
  ${native_type}* val2 = reinterpret_cast<${native_type}*>(op2->GetValue(row));\n\
  if (val1 == NULL || val2 == NULL) return NULL;\n\
  expr->result_.${result_field} = val1->${op}(*val2);\n\
  return &expr->result_.${result_field};\n\
}\n")

unary_op_template = Template("\
void* GetValueFunctions::${function_name}(Expr* e, TupleRow* row) {\n\
  ${expr_class}* expr = static_cast<${expr_class}*>(e);\n\
  // assert(p->children_.size() == 1);\n\
  Expr* op = e->children()[0];\n\
  ${native_type}* val = reinterpret_cast<${native_type}*>(op->GetValue(row));\n\
  if (val == NULL) return NULL;\n\
  expr->result_.${result_field} = ${op} *val;\n\
  return &expr->result_.${result_field};\n\
}\n")

cast_template = Template("\
void* GetValueFunctions::${function_name}(Expr* e, TupleRow* row) {\n\
  // assert(p->children_.size() == 1);\n\
  Expr* op = e->children()[0];\n\
  ${native_type}* val = reinterpret_cast<${native_type}*>(op->GetValue(row));\n\
  if (val == NULL) return NULL;\n\
  e->result_.${result_field} = *val;\n\
  return &e->result_.${result_field};\n\
}\n")

string_to_numeric_cast_template = Template("\
void* GetValueFunctions::${function_name}(Expr* e, TupleRow* row) {\n\
  // assert(p->children_.size() == 1);\n\
  Expr* op = e->children()[0];\n\
  StringValue* val = reinterpret_cast<StringValue*>(op->GetValue(row));\n\
  if (val == NULL) return NULL;\n\
  std::string tmp(val->ptr, val->len);\n\
  try {\n\
    e->result_.${result_field} = boost::lexical_cast<${result_type}>(tmp);\n\
  } catch (boost::bad_lexical_cast &) {\n\
    return NULL;\n\
  }\n\
  return &e->result_.${result_field};\n\
}\n")

numeric_to_string_cast_template = Template("\
void* GetValueFunctions::${function_name}(Expr* e, TupleRow* row) {\n\
  // assert(p->children_.size() == 1);\n\
  Expr* op = e->children()[0];\n\
  ${native_type}* val = reinterpret_cast<${native_type}*>(op->GetValue(row));\n\
  if (val == NULL) return NULL;\n\
  e->result_.SetStringVal(boost::lexical_cast<std::string>(*val));\n\
  return &e->result_.string_val;\n\
}\n")

op_invocations = [
    (unary_op_invocations, unary_op_template),
    (binary_op_invocations, binary_op_template),
    (member_fn_invocations, member_fn_template),
]

# entry: src-type, dest-type, template
cast_invocations = [
    (['char', 'short', 'int', 'long', 'float', 'double'],
     ['char', 'short', 'int', 'long', 'float', 'double'],
     cast_template),
    (['StringValue'],
     ['char', 'short', 'int', 'long', 'float', 'double'],
     string_to_numeric_cast_template),
    (['char', 'short', 'int', 'long', 'float', 'double'],
     ['StringValue'],
     numeric_to_string_cast_template)
]

cc_preamble = '\
// Copyright (c) 2011 Cloudera, Inc. All rights reserved.\n\
// This is a generated file, DO NOT EDIT IT.\n\
\n\
#include "exprs/functions.h"\n\
\n\
#include <boost/lexical_cast.hpp>\n\
#include <string>\n\
\n\
#include "exprs/arithmetic-expr.h"\n\
#include "exprs/binary-predicate.h"\n\
#include "runtime/tuple.h"\n\
\n\
namespace impala {\n\
\n'

cc_epilogue = '}\n'

h_preamble = '\
// Copyright (c) 2011 Cloudera, Inc. All rights reserved.\n\
// This is a generated file, DO NOT EDIT IT.\n\
\n\
#ifndef IMPALA_EXPRS_FUNCTIONS_H\n\
#define IMPALA_EXPRS_FUNCTIONS_H\n\
\n\
namespace impala {\n\
class Expr;\n\
class TupleRow;\n\
\n\
class GetValueFunctions {\n\
 public:\n'

h_epilogue = '\
};\n\
\n\
}\n\
\n\
#endif\n'

cc_file = open('functions.cc', 'w')
cc_file.write(cc_preamble)
h_file = open('functions.h', 'w')
h_file.write(h_preamble)

for i in op_invocations:
  for entry in i[0]:
      for op in entry[1]:
          for operand_type in entry[2]:
              d = {}
              fn_name= entry[0] + "_" + op + "_" + op_signatures[operand_type][0]
              h_file.write("  static void* " + fn_name + "(Expr* e, TupleRow* row);\n")
              d["function_name"] = fn_name
              d["expr_class"] = entry[0]
              d["native_type"] = op_signatures[operand_type][0]
              d["result_field"] = op_signatures[operand_type][1]
              d["op"] = operators[op]
              cc_file.write(i[1].substitute(d))
              cc_file.write('\n')

for i in cast_invocations:
  for src_type in i[0]:
      for dest_type in i[1]:
          if src_type == dest_type:
            continue
          d = {}
          fn_name= "Cast_" + src_type + "_" + dest_type
          h_file.write("  static void* " + fn_name + "(Expr* e, TupleRow* row);\n")
          d["function_name"] = fn_name
          d["native_type"] = src_type
          d["result_type"] = dest_type
          d["result_field"] = result_fields[dest_type]
          cc_file.write(i[2].substitute(d))
          cc_file.write('\n')

cc_file.write(cc_epilogue)
cc_file.close()
h_file.write(h_epilogue)
h_file.close()
