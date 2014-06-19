# Copyright (c) 2014 Cloudera, Inc. All rights reserved.
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

from collections import defaultdict

class Query(object):
  '''A representation of the stucture of a SQL query. Only the select_clause and
     from_clause are required for a valid query.

  '''

  def __init__(self, select_clause, from_clause):
    self.with_clause = None
    self.select_clause = select_clause
    self.from_clause = from_clause
    self.where_clause = None
    self.group_by_clause = None
    self.having_clause = None
    self.union_clause = None

  @property
  def table_exprs(self):
    '''Provides a list of all table_exprs that are declared by this query. This
       includes table_exprs in the WITH and FROM sections.
    '''
    table_exprs = self.from_clause.table_exprs
    if self.with_clause:
      table_exprs += self.with_clause.table_exprs
    return table_exprs


class SelectClause(object):
  '''This encapuslates the SELECT part of a query. It is convenient to separate
     non-agg items from agg items so that it is simple to know if the query
     is an agg query or not.

  '''

  def __init__(self, non_agg_items=None, agg_items=None):
    self.non_agg_items = non_agg_items or list()
    self.agg_items = agg_items or list()
    self.distinct = False

  @property
  def select_items(self):
    '''Provides a consolidated view of all select items.'''
    return self.non_agg_items + self.agg_items


class SelectItem(object):
  '''A representation of any possible expr than would be valid in

     SELECT <SelectItem>[, <SelectItem>...] FROM ...

     Each SelectItem contains a ValExpr which will either be a instance of a
     DataType (representing a constant), a Column, or a Func.

     Ex: "SELECT int_col + smallint_col FROM alltypes" would have a val_expr of
         Plus(Column(<alltypes.int_col>), Column(<alltypes.smallint_col>)).

  '''

  def __init__(self, val_expr, alias=None):
    self.val_expr = val_expr
    self.alias = alias

  @property
  def type(self):
    '''Returns the DataType of this item.'''
    return self.val_expr.type

  @property
  def is_agg(self):
    '''Evaluates to True if this item contains an aggregate expression.'''
    return self.val_expr.is_agg


class ValExpr(object):
  '''This is an AbstractClass that represents a generic expr that results in a
     scalar. The abc module was not used because it caused problems for the pickle
     module.

  '''

  @property
  def type(self):
    '''This is declared for documentations purposes, subclasses should override this to
       return the DataType that this expr represents.
    '''
    pass

  @property
  def base_type(self):
    '''Return the most fundemental data type that the expr evaluates to. Only
       numeric types will result in a different val than would be returned by self.type.

       Ex:
         if self.type == BigInt:
           assert self.base_type == Int
         if self.type == Double:
           assert self.base_type == Float
         if self.type == String:
           assert self.base_type == self.type
    '''
    if self.returns_int:
      return Int
    if self.returns_float:
      return Float
    return self.type

  @property
  def is_func(self):
    return isinstance(self, Func)

  @property
  def is_agg(self):
    '''Evaluates to True if this expression contains an aggregate function.'''
    if isinstance(self, AggFunc):
      return True
    if self.is_func:
      for arg in self.args:
        if arg.is_agg:
          return True

  @property
  def is_col(self):
    return isinstance(self, Column)

  @property
  def is_constant(self):
    return isinstance(self, DataType)

  @property
  def returns_boolean(self):
    return issubclass(self.type, Boolean)

  @property
  def returns_number(self):
    return issubclass(self.type, Number)

  @property
  def returns_int(self):
    return issubclass(self.type, Int)

  @property
  def returns_float(self):
    return issubclass(self.type, Float)

  @property
  def returns_string(self):
    return issubclass(self.type, String)

  @property
  def returns_timestamp(self):
    return issubclass(self.type, Timestamp)


class Column(ValExpr):
  '''A representation of a col. All TableExprs will have Columns. So a Column
     may belong to an InlineView as well as a standard Table.

     This class is used in two ways:

       1) As a piece of metadata in a table definiton. In this usage the col isn't
          intended to represent an val.

       2) As an expr in a query, for example an item being selected or as part of
          a join condition. In this usage the col is more like a val, which is why
          it implements/extends ValExpr.

  '''

  def __init__(self, owner, name, type_):
    self.owner = owner
    self.name = name
    self._type = type_

  @property
  def type(self):
    return self._type

  def __hash__(self):
    return hash(self.name)

  def __eq__(self, other):
    if not isinstance(other, Column):
      return False
    if self is other:
      return True
    return self.name == other.name and self.owner.identifier == other.owner.identifier

  def __repr__(self):
    return '%s<name: %s, type: %s>' % (
        type(self).__name__, self.name, self._type.__name__)


class FromClause(object):
  '''A representation of a FROM clause. The member variable join_clauses may optionally
     contain JoinClause items.

  '''

  def __init__(self, table_expr, join_clauses=None):
    self.table_expr = table_expr
    self.join_clauses = join_clauses or list()

  @property
  def table_exprs(self):
    '''Provides a list of all table_exprs that are declared within this FROM
       block.
    '''
    table_exprs = [join_clause.table_expr for join_clause in self.join_clauses]
    table_exprs.append(self.table_expr)
    return table_exprs


class TableExpr(object):
  '''This is an AbstractClass that represents something that a query may use to select
     from or join on. The abc module was not used because it caused problems for the
     pickle module.

  '''

  def identifier(self):
    '''Returns either a table name or alias if one has been declared.'''
    pass

  def cols(self):
    pass

  @property
  def cols_by_base_type(self):
    '''Group cols by their basic data type and return a dict of the results.

       As an example, a "BigInt" would be considered as an "Int".
    '''
    return DataType.group_by_base_type(self.cols)

  @property
  def is_table(self):
    return isinstance(self, Table)

  @property
  def is_inline_view(self):
    return isinstance(self, InlineView)

  @property
  def is_with_clause_inline_view(self):
    return isinstance(self, WithClauseInlineView)

  def __eq__(self, other):
    if not isinstance(other, type(self)):
      return False
    return self.identifier == other.identifier


class Table(TableExpr):
  '''Represents a standard database table.'''

  def __init__(self, name):
    self.name = name
    self._cols = []
    self.alias = None

  @property
  def identifier(self):
    return self.alias or self.name

  @property
  def cols(self):
    return self._cols

  @cols.setter
  def cols(self, cols):
    self._cols = cols


class InlineView(TableExpr):
  '''Represents an inline view.

     Ex: In the query "SELECT * FROM (SELECT * FROM foo) AS bar",
         "(SELECT * FROM foo) AS bar" would be an inline view.

  '''

  def __init__(self, query):
    self.query = query
    self.alias = None

  @property
  def identifier(self):
    return self.alias

  @property
  def cols(self):
    return [Column(self, item.alias, item.type) for item in
            self.query.select_clause.non_agg_items + self.query.select_clause.agg_items]


class WithClause(object):
  '''Represents a WITH clause.

     Ex: In the query "WITH bar AS (SELECT * FROM foo) SELECT * FROM bar",
         "WITH bar AS (SELECT * FROM foo)" would be the with clause.

  '''

  def __init__(self, with_clause_inline_views):
    self.with_clause_inline_views = with_clause_inline_views

  @property
  def table_exprs(self):
    return self.with_clause_inline_views


class WithClauseInlineView(InlineView):
  '''Represents the entries in a WITH clause. These are very similar to InlineViews but
     may have an additional alias.

     Ex: WITH bar AS (SELECT * FROM foo)
         SELECT *
         FROM bar as r
         JOIN (SELECT * FROM baz) AS z ON ...

         The WithClauseInlineView has aliases "bar" and "r" while the InlineView has
         only the alias "z".

  '''

  def __init__(self, query, with_clause_alias):
    self.query = query
    self.with_clause_alias = with_clause_alias
    self.alias = None

  @property
  def identifier(self):
    return self.alias or self.with_clause_alias


class JoinClause(object):
  '''A representation of a JOIN clause.

     Ex: SELECT * FROM foo <join_type> JOIN <table_expr> [ON <boolean_expr>]

     The member variable boolean_expr will be an instance of a boolean func
     defined below.

  '''

  JOINS_TYPES = ['INNER', 'LEFT', 'RIGHT', 'FULL OUTER', 'CROSS']

  def __init__(self, join_type, table_expr, boolean_expr=None):
    self.join_type = join_type
    self.table_expr = table_expr
    self.boolean_expr = boolean_expr


class WhereClause(object):
  '''The member variable boolean_expr will be an instance of a boolean func
     defined below.

  '''

  def __init__(self, boolean_expr):
    self.boolean_expr = boolean_expr


class GroupByClause(object):

  def __init__(self, select_items):
    self.group_by_items = select_items


class HavingClause(object):
  '''The member variable boolean_expr will be an instance of a boolean func
     defined below.

  '''

  def __init__(self, boolean_expr):
    self.boolean_expr = boolean_expr


class UnionClause(object):
  '''A representation of a UNION clause.

     If the member variable "all" is True, the instance represents a "UNION ALL".

  '''

  def __init__(self, query):
    self.query = query
    self.all = False

  @property
  def queries(self):
    queries = list()
    query = self.query
    while True:
      queries.append(query)
      if not query.union_clause:
        break
      query = query.union_clause.query
    return queries


class DataTypeMetaclass(type):
  '''Provides sorting of classes used to determine upcasting.'''

  def __cmp__(cls, other):
    return cmp(
        getattr(cls, 'CMP_VALUE', cls.__name__),
        getattr(other, 'CMP_VALUE', other.__name__))


class DataType(ValExpr):
  '''Base class for data types.

     Data types are represented as classes so inheritence can be used.

  '''

  __metaclass__ = DataTypeMetaclass

  def __init__(self, val):
    self.val = val

  @property
  def type(self):
    return type(self)

  @staticmethod
  def group_by_base_type(vals):
    '''Group cols by their basic data type and return a dict of the results.

       As an example, a "BigInt" would be considered as an "Int".
    '''
    vals_by_type = defaultdict(list)
    for val in vals:
      type_ = val.type
      if issubclass(type_, Int):
        type_ = Int
      elif issubclass(type_, Float):
        type_ = Float
      vals_by_type[type_].append(val)
    return vals_by_type


class Boolean(DataType):
  pass


class Number(DataType):
  pass


class Int(Number):

  # Used to compare with other numbers for determining upcasting
  CMP_VALUE = 2

  # Used during data generation to keep vals in range
  MIN = -2 ** 31
  MAX = -MIN - 1

  # Aliases used when reading and writing table definitions
  POSTGRESQL = ['INTEGER']


class TinyInt(Int):

  CMP_VALUE = 0

  MIN = -2 ** 7
  MAX = -MIN - 1

  POSTGRESQL = ['SMALLINT']


class SmallInt(Int):

  CMP_VALUE = 1

  MIN = -2 ** 15
  MAX = -MIN - 1


class BigInt(Int):

  CMP_VALUE = 3

  MIN = -2 ** 63
  MAX = -MIN - 1


class Float(Number):

  CMP_VALUE = 4

  POSTGRESQL = ['REAL']


class Double(Float):

  CMP_VALUE = 5

  MYSQL = ['DOUBLE', 'DECIMAL']   # Use double by default but add decimal synonym
  POSTGRESQL = ['DOUBLE PRECISION']


class String(DataType):

  MIN = 0
  # The Impala limit is 32,767 but MySQL has a row size limit of 65,535. To allow 3+
  # String cols per table, the limit will be lowered to 1,000. That should be fine
  # for testing anyhow.
  MAX = 1000

  MYSQL = ['VARCHAR(%s)' % MAX]
  POSTGRESQL = MYSQL + ['CHARACTER VARYING']


class Timestamp(DataType):

  MYSQL = ['DATETIME']
  POSTGRESQL = ['TIMESTAMP WITHOUT TIME ZONE']


NUMBER_TYPES = [Int, TinyInt, SmallInt, BigInt, Float, Double]
TYPES = NUMBER_TYPES + [Boolean, String, Timestamp]

class Func(ValExpr):
  '''Base class for funcs'''

  def __init__(self, *args):
    self.args = list(args)

  def __hash__(self):
    return hash(type(self)) + hash(tuple(self.args))

  def __eq__(self, other):
    if not isinstance(other, type(self)):
      return False
    if self is other:
      return True
    return self.args == other.args


class UnaryFunc(Func):

  def __init__(self, arg):
    Func.__init__(self, arg)


class BinaryFunc(Func):

  def __init__(self, left, right):
    Func.__init__(self, left, right)

  @property
  def left(self):
    return self.args[0]

  @left.setter
  def left(self, left):
    self.args[0] = left

  @property
  def right(self):
    return self.args[1]

  @right.setter
  def right(self, right):
    self.args[1] = right


class BooleanFunc(Func):

  @property
  def type(self):
    return Boolean


class IntFunc(Func):

  @property
  def type(self):
    return Int


class DoubleFunc(Func):

  @property
  def type(self):
    return Double


class StringFunc(Func):

  @property
  def type(self):
    return String


class UpcastingFunc(Func):

  @property
  def type(self):
    return max(arg.type for arg in self.args)


class AggFunc(Func):

  # Avoid having a self.distinct because it would need to be __init__'d explictly,
  # which none of the AggFunc subclasses do (ex: Avg doesn't have it's
  # own __init__).

  @property
  def distinct(self):
    return getattr(self, '_distinct', False)

  @distinct.setter
  def distinct(self, val):
    return setattr(self, '_distinct', val)

# The classes below diverge from above by including the SQL representation. It's a lot
# easier this way because there are a lot of funcs but they all have the same
# structure. Non-standard funcs, such as string concatenation, would need to have
# their representation information elsewhere (like classes above).

Parentheses = type('Parentheses', (UnaryFunc, UpcastingFunc), {'FORMAT': '({0})'})

IsNull = type('IsNull', (UnaryFunc, BooleanFunc), {'FORMAT': '{0} IS NULL'})
IsNotNull = type('IsNotNull', (UnaryFunc, BooleanFunc), {'FORMAT': '{0} IS NOT NULL'})
And = type('And', (BinaryFunc, BooleanFunc), {'FORMAT': '{0} AND {1}'})
Or = type('Or', (BinaryFunc, BooleanFunc), {'FORMAT': '{0} OR {1}'})
Equals = type('Equals', (BinaryFunc, BooleanFunc), {'FORMAT': '{0} = {1}'})
NotEquals = type('NotEquals', (BinaryFunc, BooleanFunc), {'FORMAT': '{0} != {1}'})
GreaterThan = type('GreaterThan', (BinaryFunc, BooleanFunc), {'FORMAT': '{0} > {1}'})
LessThan = type('LessThan', (BinaryFunc, BooleanFunc), {'FORMAT': '{0} < {1}'})
GreaterThanOrEquals = type(
    'GreaterThanOrEquals', (BinaryFunc, BooleanFunc), {'FORMAT': '{0} >= {1}'})
LessThanOrEquals = type(
    'LessThanOrEquals', (BinaryFunc, BooleanFunc), {'FORMAT': '{0} <= {1}'})

Plus = type('Plus', (BinaryFunc, UpcastingFunc), {'FORMAT': '{0} + {1}'})
Minus = type('Minus', (BinaryFunc, UpcastingFunc), {'FORMAT': '{0} - {1}'})
Multiply = type('Multiply', (BinaryFunc, UpcastingFunc), {'FORMAT': '{0} * {1}'})
Divide = type('Divide', (BinaryFunc, DoubleFunc), {'FORMAT': '{0} / {1}'})

Floor = type('Floor', (UnaryFunc, IntFunc), {'FORMAT': 'FLOOR({0})'})

Concat = type('Concat', (BinaryFunc, StringFunc), {'FORMAT': 'CONCAT({0}, {1})'})
Length = type('Length', (UnaryFunc, IntFunc), {'FORMAT': 'LENGTH({0})'})

ExtractYear = type(
    'ExtractYear', (UnaryFunc, IntFunc), {'FORMAT': "EXTRACT('YEAR' FROM {0})"})

# Formatting of agg funcs is a little trickier since they may have a distinct
Avg = type('Avg', (UnaryFunc, DoubleFunc, AggFunc), {})
Count = type('Count', (UnaryFunc, IntFunc, AggFunc), {})
Max = type('Max', (UnaryFunc, UpcastingFunc, AggFunc), {})
Min = type('Min', (UnaryFunc, UpcastingFunc, AggFunc), {})
Sum = type('Sum', (UnaryFunc, UpcastingFunc, AggFunc), {})

UNARY_BOOLEAN_FUNCS = [IsNull, IsNotNull]
BINARY_BOOLEAN_FUNCS = [And, Or]
RELATIONAL_OPERATORS = [
    Equals, NotEquals, GreaterThan, LessThan, GreaterThanOrEquals, LessThanOrEquals]
MATH_OPERATORS = [Plus, Minus, Multiply]  # Leaving out Divide
BINARY_STRING_FUNCS = [Concat]
AGG_FUNCS = [Avg, Count, Max, Min, Sum]


class If(Func):

  FORMAT = 'CASE WHEN {0} THEN {1} ELSE {2} END'

  def __init__(self, boolean_expr, consquent_expr, alternative_expr):
    Func.__init__(
        self, boolean_expr, consquent_expr, alternative_expr)

  @property
  def boolean_expr(self):
    return self.args[0]

  @property
  def consquent_expr(self):
    return self.args[1]

  @property
  def alternative_expr(self):
    return self.args[2]

  @property
  def type(self):
    return max((self.consquent_expr, self.alternative_expr))


class Greatest(BinaryFunc, UpcastingFunc, If):

  def __init__(self, left, rigt):
    BinaryFunc.__init__(self, left, rigt)
    If.__init__(self, GreaterThan(left, rigt), left, rigt)

  @property
  def type(self):
    return UpcastingFunc.type.fget(self)


class Cast(Func):

  FORMAT = 'CAST({0} AS {1})'

  def __init__(self, val_expr, resulting_type):
    if resulting_type not in TYPES:
      raise Exception('Unexpected type: {0}'.format(resulting_type))
    Func.__init__(self, val_expr, resulting_type)

  @property
  def val_expr(self):
    return self.args[0]

  @property
  def resulting_type(self):
    return self.args[1]

  @property
  def type(self):
    return self.resulting_type
