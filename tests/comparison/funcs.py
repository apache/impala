# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

from __future__ import absolute_import, division, print_function
from builtins import filter
from copy import deepcopy

from tests.comparison.common import ValExpr
from tests.comparison.db_types import (
    Boolean,
    Char,
    DataType,
    Decimal,
    Float,
    Int,
    Number,
    Timestamp,
    TYPES)

AGG_FUNCS = list()   # All aggregate functions will be added
ANALYTIC_FUNCS = list()   # All analytic functions will be added
FUNCS = list()   # All non-aggregate/analytic functions will be added

class Arg(object):
  '''Represents an argument in a function signature.

     data_type may be either a DataType or a list of DataTypes. A list is used to
     represent a subquery.

     If can_be_null is False, a NULL value should never be passed into the function
     during execution. This is used to maintain consistency across databases. For example
     if Impala and Postgresql both implement function foo but the results differ when
     the args to foo are NULL, then this flag can be used to prevent NULL values.

     If can_be_null_literal is False, the literal value NULL should never be an argument
     to the function. This is provided to workaround problems involving function signature
     resolution during execution. An alternative would be to CAST(NULL AS INT).

     determines_signature is used to signify that this arg is used to determine the
     signature during execution. This implies that the function has multiple signatures
     with the same number of arguments and at least one of the "determines_signature"
     arguments must be non-NULL in order to determine which signature to use during
     execution. An example is "SELECT GREATEST(NULL, NULL)" would result in an error
     during execution in Postgresql because the resulting data type could not be
     determined. An alternative would be to ensure that each modeled function contains
     the full set of possible signatures, then see if "foo(NULL)" would be ambiguous
     and if so use "foo(CAST(NULL AS INT))" instead.
  '''

  def __init__(self,
      data_type,
      require_constant=False,
      min_value=None,
      can_be_null=True,
      can_be_null_literal=True,
      determines_signature=False):
    self.type = data_type
    self.require_constant = require_constant
    self.min_value = min_value
    self.can_be_null = can_be_null
    self.can_be_null_literal = can_be_null_literal
    self.determines_signature = determines_signature

  @property
  def is_subquery(self):
    return isinstance(self.type, list)

  def validate(self, expr, skip_nulls=False):
    if not issubclass(expr.type, self.type):
      raise Exception('Expr type is %s but expected %s' % (expr.type, self.type))
    if self.require_constant and not expr.is_constant:
      raise Exception('A constant is required')
    if self.min_value is not None and expr.val < self.min_value:
      raise Exception('Minumum value not met')
    if skip_nulls and expr.is_constant and expr.val is None:
      return
    if expr.is_constant and expr.val is None and not self.can_be_null_literal:
      raise Exception('A NULL literal is not allowed')

  def __repr__(self):
    _repr = 'Arg<type: '
    if self.is_subquery:
      _repr += 'subquery[' + ', '.join([type_.__name__ for type_ in self.type]) + ']'
    else:
      _repr += self.type.__name__
    if self.require_constant:
      _repr += ', constant: True'
    if self.min_value:
      _repr += ', min: %s' % self.min_value
    _repr += '>'
    return _repr


class Signature(object):

  def __init__(self, func, return_type, *args):
    self.func = func
    self.return_type = return_type
    self.args = list(args)

  def __repr__(self):
    return "Signature<func: {func}, returns: {rt}, args: {arg_list}>".format(
        func=repr(self.func), rt=repr(self.return_type),
        arg_list=", ".join([repr(arg) for arg in self.args]))

  @property
  def input_types(self):
    return self.args[1:]


class Func(ValExpr):
  '''Base class for functions'''

  _NAME = None   # Helper for the classmethod name()
  _SIGNATURES = list()   # Helper for the classmethod signatures()

  @classmethod
  def name(cls):
    '''Returns the name of the function. Multiple functions may have the same name.
       For example, COUNT will have a separate Func class for the analytic and aggregate
       versions but both will have the same value of name().
    '''
    return cls.__name__ if cls._NAME is None else cls._NAME

  @classmethod
  def signatures(cls):
    '''Returns the available signatures for the function. Varargs are not supported, a
       subset of possible signatures must be chosen.
    '''
    return cls._SIGNATURES

  @classmethod
  def create_from_args(cls, *val_exprs):
    '''Constructor for instantiating from values. The return types of the exprs will be
       inspected and used to find the function signature. If no signature can be found
       an error will be raised.
    '''
    for signature in cls.signatures():
      if len(signature.args) != len(val_exprs):
        continue
      for idx, arg in enumerate(val_exprs):
        if not issubclass(arg.type, signature.args[idx].type):
          break
      else:
        break
    else:
      raise Exception('No signature matches the given arguments: %s' % (val_exprs, ))
    return cls(signature, *val_exprs)

  def __init__(self, signature, *val_exprs):
    '''"signature" should be one of the available signatures at the class level and
       signifies which function call this instance is intended to represent.
    '''
    if signature not in self.signatures():
      raise Exception('Unknown signature: %s' % (signature, ))
    self.signature = signature
    if val_exprs:
      self.args = list(val_exprs)
    else:
     self.args = list()
     for arg in signature.args:
       if arg.is_subquery:
         self.args.append([subtype(None) for subtype in arg.type])
       else:
         self.args.append(arg.type(arg.min_value))

  @property
  def exact_type(self):
    return self.signature.return_type

  def validate(self, skip_nulls=False):
    if not len(self.args) == len(self.signature.args):
      raise Exception('Signature length mismatch')
    for idx, signature_arg in enumerate(self.signature.args):
      signature_arg.validate(self.args[idx], skip_nulls=skip_nulls)

  def contains_subquery(self):
    for signature_arg in self.signature.args:
      if signature_arg.is_subquery:
        return True
    return any(self.iter_exprs(lambda expr: expr.is_func and expr.contains_subquery))

  def iter_exprs(self, filter=None):
    '''Returns an iterator over all val_exprs including those nested within this
       function's args.
    '''
    for arg in self.args:
      if not isinstance(arg, ValExpr):
        continue
      if not filter or filter(arg):
        yield arg
      for expr in arg.iter_exprs(filter=filter):
        yield expr

  def __hash__(self):
    return hash(type(self)) + hash(self.signature) + hash(tuple(self.args))

  def __eq__(self, other):
    if self is other:
      return True
    if not type(other) == type(self):
      return False
    return self.signature == other.signature and self.args == other.args


class AggFunc(Func):

  def __init__(self, *args):
    Func.__init__(self, *args)
    self.distinct = False

  def validate(self, skip_nulls=False):
    super(AggFunc, self).validate(skip_nulls=skip_nulls)
    for arg in self.args:
      if arg.contains_agg:
        raise Exception('Aggregate functions may not contain other aggregates')
    if self.contains_analytic:
      raise Exception('Aggregate functions may not contain analytics')


class AnalyticFunc(Func):

  HAS_IMPLICIT_WINDOW = False
  SUPPORTS_WINDOWING = True
  REQUIRES_ORDER_BY = False

  def __init__(self, *args):
    Func.__init__(self, *args)
    self.partition_by_clause = None
    self.order_by_clause = None
    self.window_clause = None

  def validate(self, skip_nulls=False):
    super(AnalyticFunc, self).validate(skip_nulls=skip_nulls)
    for arg in self.args:
      if arg.contains_analytic:
        raise Exception('Analytic functions may not contain other analytics')


class PartitionByClause(object):

  def __init__(self, val_exprs):
    self.val_exprs = val_exprs


class WindowClause(object):

  def __init__(self, range_or_rows, start_boundary, end_boundary=None):
    self.range_or_rows = range_or_rows
    self.start_boundary = start_boundary
    self.end_boundary = end_boundary


class WindowBoundary(object):

  UNBOUNDED_PRECEDING = 'UNBOUNDED PRECEDING'
  PRECEDING = 'PRECEDING'
  CURRENT_ROW = 'CURRENT ROW'
  FOLLOWING = 'FOLLOWING'
  UNBOUNDED_FOLLOWING = 'UNBOUNDED FOLLOWING'

  def __init__(self, boundary_type, val_expr=None):
    self.boundary_type = boundary_type
    self.val_expr = val_expr

# It's a lot of work to support this but it should be less error prone than explicitly
# listing each signature.
def create_func(name, returns=None, accepts=[], signatures=[], base_type=Func):
  '''Convenience function for creating a function class. The class is put into the
     global namespace just as though the class had been declared using the "class"
     keyword.

     The name of the class is "name". "base_type" can be used to specify the base class.

     The signature(s) of the class can be defined in one of three ways. "returns" and
     "accepts" can be used together but not in combination with "signatures".

       1) "signatures" should be a list of lists. Each entry corresponds to a single
          signature. Each item in the signature can be either an Arg or a DataType or
          a list of the preceding two types. The first entry in the list is the return
          type, the remainder are the input types. DataType is considered a placeholder
          for all other base types (Char, Number, Boolean, Timestamp). If a signature
          contains DataType, the entire signature will be replace with multiple
          signatures, one for each base type. Number is also considered a placeholder
          but the replacements will be the cross-product of (Int, Float, and Decimal) *
          the number of Number's used, except that the return type is the maximum of
          the input types. A function that accepts a subquery is represented by a list of
          Arg or DataType.

          Ex signatures:
            [Int, Double]: Could be a signature for FLOOR
            [Int, DataType]: Could be a signature for COUNT
                === [Int, Char] + [Int, Number] + [Int, Boolean] + ...
            [Number, Number, Number]: Could be a signature for Multiply
                === ... + [Float, Int, Float] + ... (but not [Int, Float, Float])
            [Boolean, DataType, [DataType]]: Could be a signature for In with a subquery

       2) "returns" and "accepts" is equivalent to
           signatures=[[returns, accepts[0], accepts[1], ..., accepts[n]]]

       3) "accepts" is equivalent to
           signatures=[[accepts[0], accepts[0], accepts[1], ..., accepts[n]]]
  '''
  if (returns or accepts) and signatures:
    raise Exception('Cannot mix signature specification arguments')

  type_name = base_type.__name__.replace('Func', '') + name
  func = type(type_name, (base_type, ), {'_NAME': name, '_SIGNATURES': []})
  globals()[type_name] = func

  if signatures:
    signatures = deepcopy(signatures)

  if base_type == Func:
    FUNCS.append(func)
  if returns:
    signatures = [Signature(func, returns)]
  elif accepts:
    signatures = [Signature(func, accepts[0])]
  if accepts:
    signatures[0].args.extend(accepts)

  # Replace convenience inputs with proper types
  for idx, signature in enumerate(signatures):
    if not isinstance(signature, Signature):
      signature = Signature(func, signature[0], *signature[1:])
      signatures[idx] = signature
    if isinstance(signature.return_type, Arg):
      signature.return_type = signature.return_type.type
    for arg_idx, arg in enumerate(signature.args):
      if not isinstance(arg, Arg):
        signature.args[arg_idx] = Arg(arg)

  # Replace "DataType" args with actual types
  non_wildcard_signatures = list()
  for replacement_type in TYPES:
    for signature_idx, signature in enumerate(signatures):
      replacement_signature = None
      for arg_idx, arg in enumerate(signature.args):
        if arg.is_subquery:
          for sub_idx, subtype in enumerate(arg.type):
            if subtype == DataType:
              if not replacement_signature:
                replacement_signature = deepcopy(signature)
              replacement_signature.args[arg_idx].type[sub_idx] = replacement_type
        elif arg.type == DataType:
            replacement_arg = deepcopy(arg)
            replacement_arg.type = replacement_type
            if not replacement_signature:
              replacement_signature = deepcopy(signature)
            replacement_signature.args[arg_idx] = replacement_arg
      if signature.return_type == DataType:
        if not replacement_signature:
          raise Exception('Wildcard return type requires at least one wildcard input arg')
        replacement_signature.return_type = replacement_type
      if replacement_signature:
        non_wildcard_signatures.append(replacement_signature)
      else:
        non_wildcard_signatures.append(signature)
        # This signature did not contain any "DataType" args, remove it from the list
        # so it isn't processed again.
        del signatures[signature_idx]

  # Replace "Number" args... Number wildcards work differently than DataType wildcards.
  # foo(DataType, DataType) expands to foo(Boolean, Boolean), foo(Char, Char), etc
  # but foo(Number, Number) expands to foo(Decimal, Decimal), foo(Decimal, Int), etc
  # In other words, a cross product needs to be done for Number wildcards. If the return
  # type is also "Number", then it will be replaced with the largest type of the input
  # replacements. Ex, foo(Decimal, Int) would return Decimal.

  # Find wildcard signatures
  signatures = non_wildcard_signatures
  wildcard_signatures = list()
  for signature_idx, signature in enumerate(signatures):
    is_wildcard = False
    for arg_idx, arg in enumerate(signature.args):
      if arg.is_subquery:
        for subtype in arg.type:
          if subtype == Number:
            is_wildcard = True
            break
      elif arg.type == Number:
        is_wildcard = True
      if is_wildcard:
        if signature.return_type == Number:
          signature.return_type = (Number, Int)
        wildcard_signatures.append(signature)
        del signatures[signature_idx]
        break

  # Helper function to reduce code duplication
  def update_return_type_and_append(
      replacement_type,
      replacement_signature,
      wildcard_signatures):
    if isinstance(replacement_signature.return_type, tuple):
      replacement_signature.return_type = \
          (Number, max(replacement_type, replacement_signature.return_type[1]))
    wildcard_signatures.append(replacement_signature)

  # Fully replace each wildcard one at a time so that a cross product is created
  while wildcard_signatures:
    signature = wildcard_signatures.pop()
    is_wildcard = False
    for arg_idx, arg in enumerate(signature.args):
      replacement_signature = None
      if arg.is_subquery:
        if any(filter(lambda type_: type_ == Number, arg.type)):
          raise Exception('Number not accepted in subquery signatures')
      elif arg.type == Number:
        for replacement_type in [Decimal, Int, Float]:
          replacement_signature = deepcopy(signature)
          replacement_signature.args[arg_idx].type = replacement_type
          is_wildcard = True
          update_return_type_and_append(
              replacement_type, replacement_signature, wildcard_signatures)
      if is_wildcard:
        break
    if not is_wildcard:
      if isinstance(signature.return_type, tuple):
        signature.return_type = signature.return_type[1]
      signatures.append(signature)

  func._SIGNATURES = signatures
  return func


def create_agg(name, returns=None, accepts=[], signatures=[]):
  func = create_func(name, returns, accepts, signatures, AggFunc)
  AGG_FUNCS.append(func)
  return func


def create_analytic(
    name,
    returns=None,
    accepts=[],
    signatures=[],
    require_order=False,
    supports_window=True):
  func = create_func(name, returns, accepts, signatures, AnalyticFunc)
  func.REQUIRES_ORDER_BY = require_order
  func.SUPPORTS_WINDOWING = supports_window
  ANALYTIC_FUNCS.append(func)
  return func


class CastFunc(Func):
  """
  This function is used internally by the InsertStatementGenerator to cast ValExprs into
  the proper exact types of columns.

  Arguments:
  val_expr: ValExpr to cast
  type_: Type to cast ValExpr
  """
  def __init__(self, val_expr, type_):
    self.args = [val_expr, type_]


create_func('IsNull', returns=Boolean, accepts=[DataType])
create_func('IsNotNull', returns=Boolean, accepts=[DataType])
create_func('And', returns=Boolean, accepts=[Boolean, Boolean])
create_func('Or', returns=Boolean, accepts=[Boolean, Boolean])
create_func('Exists', returns=Boolean, accepts=[[DataType]])
create_func('NotExists', returns=Boolean, accepts=[[DataType]])
for func_name in ['In', 'NotIn']:
  # Avoid equality comparison on FLOATs
  create_func(func_name, signatures=[
      [Boolean, Boolean, [Boolean]],
      [Boolean, Boolean, Boolean, Boolean],
      [Boolean, Char, [Char]],
      [Boolean, Char, Char, Char],
      [Boolean, Decimal, [Decimal]],
      [Boolean, Decimal, [Int]],
      [Boolean, Decimal, Decimal, Decimal],
      [Boolean, Decimal, Decimal, Int],
      [Boolean, Decimal, Int, Decimal],
      [Boolean, Int, [Decimal]],
      [Boolean, Int, [Int]],
      [Boolean, Int, Int, Int],
      [Boolean, Int, Decimal, Int],
      [Boolean, Int, Int, Decimal],
      [Boolean, Timestamp, [Timestamp]],
      [Boolean, Timestamp, Timestamp, Timestamp]])
for comparator in ['GreaterThan', 'LessThan']:
  create_func(comparator, signatures=[
      [Boolean, Char, Char],
      [Boolean, Number, Number],
      [Boolean, Timestamp, Timestamp]])
for comparator in ['GreaterThanOrEquals', 'LessThanOrEquals']:
  # Avoid equality comparison on FLOATs
  create_func(comparator, signatures=[
      [Boolean, Char, Char],
      [Boolean, Decimal, Decimal],
      [Boolean, Decimal, Int],
      [Boolean, Int, Decimal],
      [Boolean, Int, Int],
      [Boolean, Timestamp, Timestamp]])
for comparator in ['Equals', 'NotEquals', 'IsNotDistinctFrom', 'IsNotDistinctFromOp',
    'IsDistinctFrom']:
  # Avoid equality comparison on FLOATs
  func = create_func(comparator, signatures=[
      [Boolean, Boolean, Boolean],
      [Boolean, Char, Char],
      [Boolean, Decimal, Decimal],
      [Boolean, Decimal, Int],
      [Boolean, Int, Decimal],
      [Boolean, Int, Int],
      [Boolean, Timestamp, Timestamp]])
create_func('If', returns=DataType,
    accepts=[Boolean, Arg(DataType, determines_signature=True), DataType])

# Don't allow + or - when using floats/doubles. This is done to avoid something like
# (10000.00919 - 10000) * 10000 which would lead to random values.
for operator in ['Plus', 'Minus']:
  create_func(operator, signatures=[
    [Decimal,
     Arg(Decimal, determines_signature=True),
     Arg(Decimal, determines_signature=True)],
    [Decimal,
     Arg(Decimal, determines_signature=True),
     Arg(Int, determines_signature=True)],
    [Decimal,
     Arg(Int, determines_signature=True),
     Arg(Decimal, determines_signature=True)],
    [Int,
     Arg(Int, determines_signature=True),
     Arg(Int, determines_signature=True)]])
create_func('Multiply', signatures=[
    [Number,
     Arg(Number, determines_signature=True),
     Arg(Number, determines_signature=True)]])
# Don't allow INT / INT, Postgresql results in an INT, but a FLOAT in most other databases
create_func('Divide', signatures=[
    [Decimal,
     Arg(Decimal, determines_signature=True),
     Arg(Decimal, determines_signature=True)],
    [Decimal,
     Arg(Decimal, determines_signature=True),
     Arg(Int, determines_signature=True)],
    [Decimal,
     Arg(Int, determines_signature=True),
     Arg(Decimal, determines_signature=True)],
    [Float,
     Arg(Decimal, determines_signature=True),
     Arg(Float, determines_signature=True)],
    [Float,
     Arg(Float, determines_signature=True),
     Arg(Decimal, determines_signature=True)],
    [Float,
     Arg(Float, determines_signature=True),
     Arg(Float, determines_signature=True)],
    [Float,
     Arg(Float, determines_signature=True),
     Arg(Int, determines_signature=True)],
    [Float,
     Arg(Int, determines_signature=True),
     Arg(Float, determines_signature=True)]])

create_func('Abs', signatures=[[Number, Arg(Number, determines_signature=True)]])
# Don't allow FLOAT/DOUBLE to become an INT (ie, an approximation to be treated as a
# precise value).
create_func('Floor', signatures=[[Decimal, Decimal], [Float, Float]])
create_func('Ceil', signatures=[[Decimal, Decimal], [Float, Float]])

# NULL handling in CONCAT differs between Impala and Postgresql
create_func('Concat',
    accepts=[Arg(Char, can_be_null=False), Arg(Char, can_be_null=False)])
create_func('Trim', accepts=[Char])
create_func('Length', returns=Int, accepts=[Char])

# In order to use the levenshtein() function in Postgres this needs to be run:
# CREATE EXTENSION IF NOT EXISTS fuzzystrmatch;
create_func('Levenshtein', returns=Int, accepts=[Arg(Char), Arg(Char)])

for interval in ['Year', 'Month', 'Day', 'Hour', 'Minute', 'Second']:
  create_func('Extract' + interval,
      returns=Int, accepts=[Arg(Timestamp, can_be_null_literal=False)])
  create_func(
      'DateAdd' + interval,
      returns=Timestamp,
      # Determines signature in Postgresql
      accepts=[Arg(Timestamp, determines_signature=True), Int])

create_func('Greatest', signatures=[
    [Number,
     Arg(Number, can_be_null=False, determines_signature=True),
     Arg(Number, can_be_null=False, determines_signature=True)],
    [Timestamp,
     Arg(Timestamp, can_be_null=False, determines_signature=True),
     Arg(Timestamp, can_be_null=False, determines_signature=True)]])
create_func('Least', signatures=[
    [Number,
     Arg(Number, can_be_null=False, determines_signature=True),
     Arg(Number, can_be_null=False, determines_signature=True)],
    [Timestamp,
     Arg(Timestamp, can_be_null=False, determines_signature=True),
     Arg(Timestamp, can_be_null=False, determines_signature=True)]])
create_func('Coalesce', signatures=[
    [DataType,
     Arg(DataType, determines_signature=True),
     Arg(DataType, determines_signature=True)],
    [DataType,
     Arg(DataType, determines_signature=True),
     Arg(DataType, determines_signature=True),
     Arg(DataType, determines_signature=True)]])

# This is added so that query generation can assume that any return type can be
# produced by an aggregate or analytic with only one level of nesting.
# Ex: CAST(SUM(...) AS STRING)
create_func('CastAsChar', signatures=[[Char, Int]])

create_agg('Count', returns=Int, accepts=[Number])
create_agg('Max', signatures=[
  [Number, Arg(Number, determines_signature=True)],
  [Timestamp, Arg(Timestamp, determines_signature=True)]])
create_agg('Min', signatures=[
  [Number, Arg(Number, determines_signature=True)],
  [Timestamp, Arg(Timestamp, determines_signature=True)]])
create_agg('Sum', signatures=[
  # FLOATs not allowed. See comment about Plus/Minus for info.
  [Int, Arg(Int, determines_signature=True)],
  [Decimal, Arg(Decimal, determines_signature=True)]])
create_agg('Avg', signatures=[
  [Float, Arg(Int, determines_signature=True)],
  [Decimal, Arg(Decimal, determines_signature=True)]])

create_analytic('Rank', require_order=True, supports_window=False, returns=Int)
create_analytic('DenseRank', require_order=True, supports_window=False, returns=Int)
create_analytic('RowNumber', require_order=True, supports_window=False, returns=Int)
create_analytic('Lead', require_order=True, supports_window=False, signatures=[
  [DataType, Arg(DataType, determines_signature=True)],
  [DataType,
   Arg(DataType, determines_signature=True),
   Arg(Int, require_constant=True, min_value=1)]])
create_analytic('Lag', require_order=True, supports_window=False, signatures=[
  [DataType, Arg(DataType, determines_signature=True)],
  [DataType,
   Arg(DataType, determines_signature=True),
   Arg(Int, require_constant=True, min_value=1)]])
create_analytic('FirstValue', require_order=True, signatures=[
    [DataType, Arg(DataType, determines_signature=True)]])
create_analytic('LastValue', require_order=True, signatures=[
    [DataType, Arg(DataType, determines_signature=True)]])
create_analytic('Max', signatures=[
  [Number, Arg(Number, determines_signature=True)],
  [Timestamp, Arg(Timestamp, determines_signature=True)]])
create_analytic('Min', signatures=[[Number, Number], [Timestamp, Timestamp]])
create_analytic('Sum', signatures=[[Int, Int], [Decimal, Decimal]])  # FLOATs not allowed
create_analytic('Count', returns=Int, accepts=[Number])
create_analytic('Avg', returns=Float, accepts=[Number])
