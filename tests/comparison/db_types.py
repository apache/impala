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
from future.utils import with_metaclass
import re
import sys

from collections import defaultdict

from tests.comparison.common import ValExpr, ValExprList

module_contents = dict()

class DataTypeMetaclass(type):
  '''Provides sorting of classes used to determine upcasting.'''

  def __init__(cls, name, bases, dict):
    super(DataTypeMetaclass, cls).__init__(name, bases, dict)
    if name in ('Char', 'DataType', 'Decimal', 'Float', 'Int', 'Number', 'Timestamp'):
      cls.type = cls
    else:
      cls.type = cls.get_generic_type()

  def __cmp__(cls, other):
    if not isinstance(other, DataTypeMetaclass):
      return -1
    return cmp(
        getattr(cls, 'CMP_VALUE', cls.__name__),
        getattr(other, 'CMP_VALUE', other.__name__))


class DataType(with_metaclass(DataTypeMetaclass, ValExpr)):
  '''Base class for data types.

     Data types are represented as classes so inheritance can be used.

  '''

  @staticmethod
  def group_by_type(vals):
    '''Group cols by their data type and return a dict of the results.'''
    vals_by_type = defaultdict(ValExprList)
    for val in vals:
      vals_by_type[val.type].append(val)
    return vals_by_type

  @classmethod
  def get_base_type(cls):
    '''This should only be called from a subclass to find the type that is just below
       DataType in the class hierarchy. For example Int and Decimal would both return
       Number as their base type.
    '''
    if DataType in cls.__bases__:
      return cls
    for base in cls.__bases__:
      if issubclass(base, DataType):
        return base.get_base_type()
    raise Exception('Unable to determine base type of %s' % cls)

  @classmethod
  def get_generic_type(cls):
    return cls.get_base_type()

  @classmethod
  def name(cls):
    return cls.__name__

  @classmethod
  def is_approximate(cls):
    return False

  def __init__(self, val):
    self.val = val

  @property
  def exact_type(self):
    return type(self)



class Boolean(DataType):
  pass


class Number(DataType):
  pass


class Int(Number):

  @classmethod
  def get_generic_type(cls):
    return Int

  # Used to compare with other numbers for determining upcasting
  CMP_VALUE = 2

  # Used during data generation to keep vals in range
  MIN = -2 ** 31
  MAX = -MIN - 1


class TinyInt(Int):

  CMP_VALUE = 0

  MIN = -2 ** 7
  MAX = -MIN - 1


class SmallInt(Int):

  CMP_VALUE = 1

  MIN = -2 ** 15
  MAX = -MIN - 1


class BigInt(Int):

  CMP_VALUE = 3

  MIN = -2 ** 63
  MAX = -MIN - 1


class Decimal(Number):

  @classmethod
  def get_generic_type(cls):
    return Decimal

  CMP_VALUE = 4

  MAX_DIGITS = 38   # Arbitrary default values
  MAX_FRACTIONAL_DIGITS = 10   # Arbitrary default values


class Float(Number):

  @classmethod
  def get_generic_type(cls):
    return Float

  @classmethod
  def is_approximate(cls):
    return True

  CMP_VALUE = 5


class Double(Float):

  CMP_VALUE = 6


class Char(DataType):

  CMP_VALUE = 100

  MIN = 0
  MAX = 255   # This is not the true max


class VarChar(Char):

  CMP_VALUE = 101

  MAX = 255   # Not a true max. This is used to differentiate between VarChar and String.


class String(VarChar):

  CMP_VALUE = 102

  MIN = VarChar.MAX + 1   # This is used to differentiate between VarChar and String.
  MAX = 1000   # This is not the true max.


class Timestamp(DataType):
  pass


EXACT_TYPES = [
    BigInt,
    Boolean,
    Char,
    Decimal,
    Double,
    Float,
    Int,
    SmallInt,
    String,
    Timestamp,
    TinyInt,
    VarChar]
JOINABLE_TYPES = (Char, Decimal, Int, Timestamp)
TYPES = tuple(set(type_.type for type_ in EXACT_TYPES))

__DECIMAL_TYPE_CACHE = dict()
def get_decimal_class(total_digits, fractional_digits):
  cache_key = (total_digits, fractional_digits)
  if cache_key not in __DECIMAL_TYPE_CACHE:
    __DECIMAL_TYPE_CACHE[cache_key] = type(
        'Decimal%02d%02d' % (total_digits, fractional_digits),
        (Decimal, ),
        {'MAX_DIGITS': total_digits, 'MAX_FRACTIONAL_DIGITS': fractional_digits})
  return __DECIMAL_TYPE_CACHE[cache_key]


__CHAR_TYPE_CACHE = dict()
def get_char_class(length):
  if length not in __CHAR_TYPE_CACHE:
    __CHAR_TYPE_CACHE[length] = type(
        'Char%04d' % length,
        (Char, ),
        {'MAX': length})
  return __CHAR_TYPE_CACHE[length]


__VARCHAR_TYPE_CACHE = dict()
def get_varchar_class(length):
  if length not in __VARCHAR_TYPE_CACHE:
    __VARCHAR_TYPE_CACHE[length] = type(
        'VarChar%04d' % length,
        (VarChar, ),
        {'MAX': length})
  return __VARCHAR_TYPE_CACHE[length]


class ModuleWrapper(object):

  def __init__(self, module):
    self.module = module
    self.decimal_class_pattern = re.compile(r"Decimal(\d{2})(\d{2})")
    self.char_class_pattern = re.compile(r"Char(\d+)")
    self.varchar_class_pattern = re.compile(r"VarChar(\d+)")

  def __getattr__(self, name):
    match = self.decimal_class_pattern.match(name)
    if match:
      return self.get_decimal_class(int(match.group(1)), int(match.group(2)))
    match = self.char_class_pattern.match(name)
    if match:
      return self.get_char_class(int(match.group(1)))
    match = self.varchar_class_pattern.match(name)
    if match:
      return self.get_varchar_class(int(match.group(1)))
    return getattr(self.module, name)


sys.modules[__name__] = ModuleWrapper(sys.modules[__name__])
