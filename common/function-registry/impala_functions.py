#!/usr/bin/env python

# This is a list of all the functions that are not auto-generated.
# It contains all the meta data that describes the function.  The format is:
# <function name>, <return_type>, [<args>], <backend function name>, [<sql function aliases>]
#
# 'function name' is the base of what the opcode enum will be generated from.  It does not
# have to be unique, the script will mangle the name with the signature if necessary.
#
# 'sql function aliases' are the function names that can be used from sql.  They are optional
# and there can be multiple aliases for a function.
#
# This is combined with the list in generated_functions to code-gen the opcode
# registry in the FE and BE.

functions = [
  ['Compound_And', 'BOOLEAN', ['BOOLEAN', 'BOOLEAN'], 'CompoundPredicate::AndComputeFunction', []],
  ['Compound_Or', 'BOOLEAN', ['BOOLEAN', 'BOOLEAN'], 'CompoundPredicate::OrComputeFunction', []],
  ['Compound_Not', 'BOOLEAN', ['BOOLEAN', 'BOOLEAN'], 'CompoundPredicate::NotComputeFunction', []],

  ['Constant_Regex', 'BOOLEAN', ['BOOLEAN', 'BOOLEAN'], 'LikePredicate::ConstantRegexFn', []],
  ['Constant_Substring', 'BOOLEAN', ['BOOLEAN', 'BOOLEAN'], 'LikePredicate::ConstantSubstringFn', []],
  ['Like', 'BOOLEAN', ['STRING', 'STRING'], 'LikePredicate::LikeFn', []],
  ['Regex', 'BOOLEAN', ['STRING', 'STRING'], 'LikePredicate::RegexFn', []],

  ['Math_Pi', 'DOUBLE', [], 'MathFunctions::Pi', ['pi']],

  ['String_Substring', 'STRING', ['STRING', 'INT'], 'StringFunctions::Substring', ['substr', 'substring']],
  ['String_Substring', 'STRING', ['STRING', 'INT', 'INT'], 'StringFunctions::Substring', ['substr', 'substring']],
# left and right are key words, leave them out for now.
  ['String_Left', 'STRING', ['STRING', 'INT'], 'StringFunctions::Left', ['strleft']],
  ['String_Right', 'STRING', ['STRING', 'INT'], 'StringFunctions::Right', ['strright']],
  ['String_Length', 'INT', ['STRING'], 'StringFunctions::Length', ['length']],
  ['String_Lower', 'STRING', ['STRING'], 'StringFunctions::Lower', ['lower', 'lcase']],
  ['String_Upper', 'STRING', ['STRING'], 'StringFunctions::Upper', ['upper', 'ucase']],
  ['String_Reverse', 'STRING', ['STRING'], 'StringFunctions::Reverse', ['reverse']],
]
