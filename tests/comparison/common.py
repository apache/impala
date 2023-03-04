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
import json

from collections import defaultdict
from copy import deepcopy

# The model related modules (types.py, query.py, etc) are interconnected by circular
# imports which causes problems for the python import system. This module is intended to
# be the first of the circular modules imported. To be importable, no direct references
# are made to the other modules from this modules namespace. Instead, other modules are
# lazyily imported using the following function. Keep in mind that python "globals" are
# module local, there is no such thing as a cross-module global.
__ALREADY_IMPORTED = False


def get_import(name):
  # noqa below tells flake8 to not warn when it thinks imports are not used
  global __ALREADY_IMPORTED
  if not __ALREADY_IMPORTED:
    from tests.comparison.db_types import (  # noqa
        BigInt,
        Boolean,
        Char,
        DataType,
        Decimal,
        Float,
        Int,
        JOINABLE_TYPES,
        Number,
        Timestamp)
    from tests.comparison.funcs import AggFunc, AnalyticFunc, Func  # noqa
    from tests.comparison.query import InlineView, Subquery, WithClauseInlineView  # noqa
    for key, value in locals().items():
      globals()[key] = value
    __ALREADY_IMPORTED = True
  return globals()[name]


class ValExpr(object):
  '''This is class that represents a generic expr that results in a scalar.'''

  @property
  def type(self):
    '''Returns the type that this expr evaluates to. The type may be Int or Char but
       never BigInt or String. Valid return value are the set defined in types.TYPES.
    '''
    return self.exact_type.get_generic_type()

  @property
  def exact_type(self):
    '''Return the actual type of the val expr. For example "type" could return Int and
       "exact_type" could return TinyInt.
    '''
    pass

  @property
  def base_type(self):
    '''Returns the lowest type in the type heirarchy that is not DataType. For non-
       numeric types, the return value will be the same is self.type. Numeric types
       will return Number whereas self.type may be Decimal, Int, or Float.
    '''
    return self.type.get_base_type()

  @property
  def is_func(self):
    '''Evaluates to True if this expr is an instance of a function.'''
    return isinstance(self, get_import('Func'))

  @property
  def is_agg(self):
    '''Evaluates to True if this expr is an instance of an aggregate function.'''
    return isinstance(self, get_import('AggFunc'))

  @property
  def is_analytic(self):
    '''Evaluates to True if this expr is an instance of an analytic function.'''
    return isinstance(self, get_import('AnalyticFunc'))

  @property
  def contains_agg(self):
    '''Evaluates to True if this expression is an aggregate function or contains an
       aggregate function.
    '''
    return self.is_agg or self.is_func and any(
        isinstance(arg, ValExpr) and arg.contains_agg for arg in self.args)

  @property
  def contains_analytic(self):
    '''Evaluates to True if this expression is an analytic function or contains an
       analytic function.
    '''
    return self.is_analytic or self.is_func and any(
        isinstance(arg, ValExpr) and arg.contains_analytic for arg in self.args)

  @property
  def contains_subquery(self):
    '''Evaluates to True if this expression is a subquery or contains a subquery.'''
    return self.is_subquery or self.is_func and any(
        isinstance(arg, ValExpr) and arg.contains_subquery for arg in self.args)

  @property
  def is_col(self):
    return isinstance(self, Column)

  @property
  def is_constant(self):
    return isinstance(self, get_import('DataType'))

  @property
  def is_subquery(self):
    return isinstance(self, get_import('Subquery'))

  @property
  def returns_boolean(self):
    return issubclass(self.type, get_import('Boolean'))

  @property
  def returns_int(self):
    return issubclass(self.type, get_import('Int'))

  @property
  def returns_float(self):
    return issubclass(self.type, get_import('Float'))

  @property
  def returns_char(self):
    return issubclass(self.type, get_import('Char'))

  @property
  def returns_timestamp(self):
    return issubclass(self.type, get_import('Timestamp'))

  def iter_exprs(self, filter=None):
    '''Return an iterator over all exprs that this expr contains as a function argument
       including this expr itself.
    '''
    if not filter or filter(self):
      yield self

  def count_col_refs(self):
    '''Return a dict with Columns as keys and the number of times the column was used
       in this expr as values.
    '''
    col_ref_counts = defaultdict(int)
    if self.is_func:
      for arg in self.args:
        if isinstance(arg, ValExpr):
          for col, count in arg.count_col_refs().items():
            col_ref_counts[col] += count
    elif self.is_col:
      col_ref_counts[self] += 1
    return col_ref_counts


class StructColumn(object):
  '''The methods in this class are similar to TableExpr.

     In Impala it's not possible to select from a struct column. To mirror this behavior
     here, StructColumn does not inherit from TableExpr.

     TODO: Maybe make a parent class that both StructColumn and TableExpr will inhert
           from.
  '''

  def __init__(self, owner, name):
    self.owner = owner
    self.name = name
    self._cols = []
    self.alias = None

  @property
  def identifier(self):
    return self.alias or self.name

  @property
  def cols(self):
    '''Returns a ValExprList containing all scalar cols in this StructColumn. '''
    # TODO: Since Impala now supports nested types, this method can be renamed to
    # scalar_cols
    result = ValExprList()
    for col in self._cols:
      if not isinstance(col, CollectionColumn):
        result.extend(col.cols)
    return result

  @property
  def collections(self):
    result = []
    for col in self._cols:
      if isinstance(col, CollectionColumn):
        result.append(col)
        result.extend(col.collections)
      elif isinstance(col, StructColumn):
        result.extend(col.collections)
    return result

  def get_col_by_name(self, col_name):
    for col in self._cols:
      if col.name == col_name:
        return col
    return None

  def add_col(self, col):
    col.owner = self
    self._cols.append(col)

  def __eq__(self, other):
    if not isinstance(other, StructColumn):
      return False
    if self is other:
      return True
    return self.name == other.name and self.owner.identifier == other.owner.identifier

  def __hash__(self):
    return hash(self.name)

  def __repr__(self):
    cols_str = ', '.join(str(f) for f in self._cols)
    return '%s<name: %s, cols: [%s]>' % (type(self).__name__, self.name, cols_str)

  def __deepcopy__(self, memo):
    other = StructColumn(self.owner, self.name)
    other.alias = self.alias
    for col in self._cols:
      other.add_col(deepcopy(col, memo))
    return other


class Column(ValExpr):
  '''A representation of a column. All TableExprs will have Columns. So a Column
     may belong to an InlineView as well as a standard Table.

     This class is used in two ways:

       1) As a piece of metadata in a table definiton. In this usage the col isn't
          intended to represent an val.

       2) As an expr in a query, for example an item being selected or as part of
          a JOIN condition. In this usage the col is more like a val, which is why
          it implements/extends ValExpr.

     This class can also be used to represent Map keys, Map values, Array pos,
     scalar struct field, and scalar array item.
  '''

  def __init__(self, owner, name, exact_type):
    self.owner = owner
    self.name = name
    self._exact_type = exact_type
    self.is_primary_key = False

  @property
  def exact_type(self):
    return self._exact_type

  @exact_type.setter
  def exact_type(self, exact_type):
    self._exact_type = exact_type

  def __hash__(self):
    return hash(self.name)

  def __eq__(self, other):
    if not isinstance(other, Column):
      return False
    if self is other:
      return True
    return self.name == other.name and self.owner.identifier == other.owner.identifier

  @property
  def cols(self):
    return ValExprList([self])

  def __repr__(self):
    return '%s<name: %s, type: %s>' % (
        type(self).__name__, self.name, self.exact_type.__name__)

  def __deepcopy__(self, memo):
    # Don't return a deep copy of owner, since that is a circular reference
    return Column(self.owner, self.name, self.exact_type)


class ValExprList(list):
  '''A list of ValExprs.'''

  @property
  def by_type(self):
    return get_import('DataType').group_by_type(self)

  def __repr__(self):
    return 'ValExprList: ' + ', '.join(str(x) for x in self)


class TableExpr(object):
  '''This class represents something that a query may use to SELECT from or JOIN on.'''

  @property
  def identifier(self):
    '''Returns either a table name or alias if one has been declared.'''
    pass

  @property
  def unique_cols(self):
    '''Returns a list of lists of Cols that in combination define a unique set of values
       within the table. The returned list could be thought of as a list of uniqueness
       constraints (though there may be no actual constraints or any other type of
       enforcement).
    '''
    return ValExprList()

  @property
  def joinable_cols(self):
    '''Returns a list of Cols that are of a type that is allowed in a JOIN. This is
       mostly an Impala specific thing since Impala requires at least one equality based
       join and not all types are allowed in equality comparisons. Also Boolean is
       excluded because of low cardinality.
    '''
    joinable_types = tuple(get_import('JOINABLE_TYPES'))
    return ValExprList(col for col in self.cols if issubclass(col.type, joinable_types))

  @property
  def col_types(self):
    '''Returns a Set containing the various column types that this TableExpr contains.'''
    return set(self.cols_by_type)

  @property
  def collections(self):
    '''Returns all nested collections that can be accessed from this TableExpr.'''
    result = []
    for col in self._cols:
      if isinstance(col, CollectionColumn):
        result.append(col)
        result.extend(col.collections)
      elif isinstance(col, StructColumn):
        result.extend(col.collections)
    return result

  def add_col(self, col):
    col.owner = self
    self._cols.append(col)

  def is_visible(self):
    '''If False is returned, columns from this TableExpr may only be used in JOIN
       conditions. This is intended to be used to identify ANTI and SEMI joined table
       exprs.
    '''
    pass

  @property
  def cols_by_type(self):
    '''Group cols of the same type into lists and return a dict of the results.'''
    return get_import('DataType').group_by_type(self.cols)

  @property
  def joinable_cols_by_type(self):
    return get_import('DataType').group_by_type(self.joinable_cols)

  @property
  def is_table(self):
    return isinstance(self, Table)

  @property
  def is_inline_view(self):
    return isinstance(self, get_import('InlineView'))

  @property
  def is_with_clause_inline_view(self):
    return isinstance(self, get_import('WithClauseInlineView'))

  def __hash__(self):
    return hash(self.identifier)

  def __eq__(self, other):
    if not isinstance(other, type(self)):
      return False
    return self.identifier == other.identifier


class CollectionColumn(TableExpr):
  '''Used for representing Map or Array columns.'''

  def __init__(self, owner, name):
    self.name = name
    # Owner can be one of: Table, ArrayColumn or StructColumn.
    self.owner = owner
    self.is_visible = True
    self.alias = None
    self._cols = []

  @property
  def identifier(self):
    return self.alias or self.name

  @property
  def cols(self):
    result = ValExprList()
    for col in self._cols:
      if not isinstance(col, CollectionColumn):
        result.extend(col.cols)
    return result

  def get_col_by_name(self, col_name):
    for col in self._cols:
      if col.name == col_name:
        return col
    return None

  def __hash__(self):
    return hash(self.name)

  def __repr__(self):
    cols_str = ', '.join(str(f) for f in self._cols)
    return '%s<name: %s, cols: [%s]>' % (type(self).__name__, self.name, cols_str)


class ArrayColumn(CollectionColumn):

  def __init__(self, owner, name, item):
    '''Item represents the type of array. For example if array type is Int, item should be
       Column of type Int.
    '''
    super(ArrayColumn, self).__init__(owner, name)
    item.owner = self
    item.name = 'item'
    self._cols.append(item)
    # Arrays have 2 fields: pos and item. Pos is automatically set to BigInt.
    self._cols.append(Column(
        owner=self, name='pos', exact_type=get_import('BigInt')))

  def __eq__(self, other):
    if not isinstance(other, ArrayColumn):
      return False
    if self is other:
      return True
    return self.name == other.name and self.owner.identifier == other.owner.identifier

  def __hash__(self):
    return hash((self.name, self.owner.identifier))

  def __deepcopy__(self, memo):
    other = ArrayColumn(
        owner=self.owner,
        name=self.name,
        item=deepcopy(self.get_col_by_name('item')))
    other.alias = self.alias
    return other


class MapColumn(CollectionColumn):

  def __init__(self, owner, name, key, value):
    super(MapColumn, self).__init__(owner, name)
    # Set key
    key.owner = self
    key.name = 'key'
    self._cols.append(key)
    # Set value
    value.owner = self
    value.name = 'value'
    self._cols.append(value)

  def __eq__(self, other):
    if not isinstance(other, MapColumn):
      return False
    if self is other:
      return True
    return self.name == other.name and self.owner.identifier == other.owner.identifier

  def __hash__(self):
    return hash((self.name, self.owner.identifier))

  def __deepcopy__(self, memo):
    other = MapColumn(
        owner=self.owner,
        name=self.name,
        key=deepcopy(self.get_col_by_name('key')),
        value=deepcopy(self.get_col_by_name('value')))
    other.alias = self.alias
    return other


class Table(TableExpr):
  '''Represents a standard database table.'''

  def __init__(self, name):
    self.name = name
    self._cols = []  # can include CollectionColumns and StructColumns
    self._unique_cols = []
    self.alias = None
    self.is_visible = True   # Tables used in SEMI or ANTI JOINs are invisible

    # Only used for data loading. Always stored in upper-case. If set, values will be
    # something like 'PARQUET' or 'TEXT'. See cli_options.py for a full list of
    # possible values.
    self._storage_format = None

    # Only used for data loading. For Impala and Hive, this is the path to the directory
    # in the storage system, such as an HDFS URL.
    self.storage_location = None

    # Only used for data loading. Avro tables may require a separate schema definition,
    # this is the path to the schema file in the storage system, such as an HDFS URL.
    self.schema_location = None

  @property
  def identifier(self):
    return self.alias or self.name

  @property
  def primary_keys(self):
    """
    Return immutable sequence of primary keys.
    """
    return tuple(col for col in self._cols if col.is_primary_key)

  @property
  def primary_key_names(self):
    """
    Return immutable sequence for primary key names.
    """
    return tuple(col.name for col in self.primary_keys)

  @property
  def updatable_columns(self):
    """
    Return immutable sequence of columns that may be updated (i.e., not primary keys).

    If the table doesn't have primary keys, no columns are updatable.
    """
    if self.primary_keys:
      return tuple(col for col in self._cols if not col.is_primary_key)
    else:
      return ()

  @property
  def updatable_column_names(self):
    """
    Return immutable sequence of column names that may be updated
    """
    return tuple(col.name for col in self.updatable_columns)

  @property
  def cols(self):
    result = ValExprList()
    for col in self._cols:
      if not isinstance(col, CollectionColumn):
        result.extend(col.cols)
    return result

  @cols.setter
  def cols(self, cols):
    self._cols = cols

  @property
  def unique_cols(self):
    return self._unique_cols

  @unique_cols.setter
  def unique_cols(self, unique_cols):
    self._unique_cols = unique_cols

  @property
  def storage_format(self):
    return self._storage_format

  @storage_format.setter
  def storage_format(self, storage_format):
    self._storage_format = storage_format and storage_format.upper()

  def get_avro_schema(self):
    avro_schema = {'name': 'my_record', 'type': 'record', 'fields': []}
    for col in self.cols:
      if issubclass(col.type, get_import('Int')):
        avro_type = 'int'
      elif issubclass(col.type, get_import('Char')):
        avro_type = 'string'
      elif issubclass(col.type, get_import('Decimal')):
        avro_type = {
            "type": "bytes",
            "logicalType": "decimal",
            "precision": col.exact_type.MAX_DIGITS,
            "scale": col.exact_type.MAX_FRACTIONAL_DIGITS}
      else:
        avro_type = col.type.__name__.lower()
      avro_schema['fields'].append({'name': col.name, 'type': ['null', avro_type]})
    return json.dumps(avro_schema)

  def __repr__(self):
    return 'Table<name: %s, cols: %s>' \
        % (self.name, ', '.join([str(col) for col in self._cols]))

  def __deepcopy__(self, memo):
    other = Table(self.name)
    other.alias = self.alias
    other.is_visible = self.is_visible
    cols_by_name = dict()
    for col in self._cols:
      result_col = deepcopy(col, memo)
      other.add_col(result_col)
      cols_by_name[result_col.name] = result_col

    other._unique_cols = []
    for col_combo in self._unique_cols:
      other_col_combo = set()
      for col in col_combo:
        other_col_combo.add(cols_by_name[col.name])
      other.unique_cols.append(other_col_combo)
    return other


class TableExprList(list):
  '''A list of TableExprs.'''

  @property
  def cols(self):
    '''Return a list of all the Columns containd in all the TableExprs.'''
    return ValExprList(col for table_expr in self for col in table_expr.cols)

  @property
  def joinable_cols_by_type(self):
    cols_by_type = defaultdict(ValExprList)
    for table_expr in self:
      for type_, cols in table_expr.joinable_cols_by_type.items():
        cols_by_type[type_].extend(cols)
    return cols_by_type

  @property
  def cols_by_type(self):
    cols_by_type = defaultdict(ValExprList)
    for table_expr in self:
      for type_, cols in table_expr.cols_by_type.items():
        cols_by_type[type_].extend(cols)
    return cols_by_type

  @property
  def col_types(self):
    return tuple(self.cols_by_type)

  @property
  def collections(self):
    result = []
    for table_expr in self:
      result.extend(table_expr.collections)
    return result

  @property
  def by_col_type(self):
    '''Return a dict with keys being column types and values being lists of TableExprs
       that have at least one Column of that type.
    '''
    table_exprs_by_type = defaultdict(TableExprList)
    for table_expr in self:
      for col_type in table_expr.col_types:
        table_exprs_by_type[col_type].append(table_expr)
    return table_exprs_by_type
