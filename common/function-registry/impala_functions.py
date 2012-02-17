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
  ['Math_E', 'DOUBLE', [], 'MathFunctions::E', ['e']],
  ['Math_Abs', 'DOUBLE', ['DOUBLE'], 'MathFunctions::Abs', ['abs']],
  ['Math_Sign', 'FLOAT', ['DOUBLE'], 'MathFunctions::Sign', ['sign']],
  ['Math_Sin', 'DOUBLE', ['DOUBLE'], 'MathFunctions::Sin', ['sin']],
  ['Math_Asin', 'DOUBLE', ['DOUBLE'], 'MathFunctions::Asin', ['asin']],
  ['Math_Cos', 'DOUBLE', ['DOUBLE'], 'MathFunctions::Cos', ['cos']],
  ['Math_Acos', 'DOUBLE', ['DOUBLE'], 'MathFunctions::Acos', ['acos']],
  ['Math_Tan', 'DOUBLE', ['DOUBLE'], 'MathFunctions::Tan', ['tan']],
  ['Math_Atan', 'DOUBLE', ['DOUBLE'], 'MathFunctions::Atan', ['atan']],
  ['Math_Radians', 'DOUBLE', ['DOUBLE'], 'MathFunctions::Radians', ['radians']],
  ['Math_Degrees', 'DOUBLE', ['DOUBLE'], 'MathFunctions::Degrees', ['degrees']],
  ['Math_Ceil', 'BIGINT', ['DOUBLE'], 'MathFunctions::Ceil', ['ceil', 'ceiling']],
  ['Math_Floor', 'BIGINT', ['DOUBLE'], 'MathFunctions::Floor', ['floor']],
  ['Math_Round', 'BIGINT', ['DOUBLE'], 'MathFunctions::Round', ['round']],
  ['Math_Round', 'DOUBLE', ['DOUBLE', 'INT'], 'MathFunctions::RoundUpTo', ['round']],
  ['Math_Exp', 'DOUBLE', ['DOUBLE'], 'MathFunctions::Exp', ['exp']],
  ['Math_Ln', 'DOUBLE', ['DOUBLE'], 'MathFunctions::Ln', ['ln']],
  ['Math_Log10', 'DOUBLE', ['DOUBLE'], 'MathFunctions::Log10', ['log10']],
  ['Math_Log2', 'DOUBLE', ['DOUBLE'], 'MathFunctions::Log2', ['log2']],
  ['Math_Log', 'DOUBLE', ['DOUBLE', 'DOUBLE'], 'MathFunctions::Log', ['log']],
  ['Math_Pow', 'DOUBLE', ['DOUBLE', 'DOUBLE'], 'MathFunctions::Pow', ['pow', 'power']],
  ['Math_Sqrt', 'DOUBLE', ['DOUBLE'], 'MathFunctions::Sqrt', ['sqrt']],
  ['Math_Rand', 'DOUBLE', [], 'MathFunctions::Rand', ['rand']],
  ['Math_Rand', 'DOUBLE', ['INT'], 'MathFunctions::RandSeed', ['rand']],
  ['Math_Bin', 'STRING', ['BIGINT'], 'MathFunctions::Bin', ['bin']],
  ['Math_Hex', 'STRING', ['BIGINT'], 'MathFunctions::HexInt', ['hex']],
  ['Math_Hex', 'STRING', ['STRING'], 'MathFunctions::HexString', ['hex']],
  ['Math_Unhex', 'STRING', ['STRING'], 'MathFunctions::Unhex', ['unhex']],
  ['Math_Conv', 'STRING', ['BIGINT', 'TINYINT', 'TINYINT'], 'MathFunctions::ConvInt', ['conv']],
  ['Math_Conv', 'STRING', ['STRING', 'TINYINT', 'TINYINT'], 'MathFunctions::ConvString', ['conv']],
  ['Math_Pmod', 'BIGINT', ['BIGINT', 'BIGINT'], 'MathFunctions::PmodBigInt', ['pmod']],
  ['Math_Pmod', 'DOUBLE', ['DOUBLE', 'DOUBLE'], 'MathFunctions::PmodDouble', ['pmod']],
  ['Math_Positive', 'BIGINT', ['BIGINT'], 'MathFunctions::PositiveBigInt', ['positive']],
  ['Math_Positive', 'DOUBLE', ['DOUBLE'], 'MathFunctions::PositiveDouble', ['positive']],
  ['Math_Negative', 'BIGINT', ['BIGINT'], 'MathFunctions::NegativeBigInt', ['negative']],
  ['Math_Negative', 'DOUBLE', ['DOUBLE'], 'MathFunctions::NegativeDouble', ['negative']],

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
