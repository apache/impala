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
#
# Validates that casting to Decimal works.
#
import logging
import pytest
from decimal import Decimal, getcontext, ROUND_DOWN
from metacomm.combinatorics.all_pairs2 import all_pairs2 as all_pairs
from random import shuffle, randint, seed
from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_vector import TestDimension, TestMatrix

class TestDecimalCasting(ImpalaTestSuite):
  """Test Suite to verify that casting to Decimal works.

  Specifically, this test suite ensures that:
    - overflows and underflows and handled correctly.
    - casts from floats/string to their exact decimal types are correct.
    - max/min/NULL/0 can be expressed with their respective decimal types.
  """
  DECIMAL_TYPES_MAP = {
      # All possible decimal types.
      # (0 < precision <= 38 && 0 <= scale <= 38 && scale <= precision)
      'exhaustive' : [(p, s) for p in xrange(1, 39) for s in xrange(0, p + 1)],
      # Core only deals with precision 6,16,26 (different integer types)
      'core' :  [(p, s) for p in [6,16,26] for s in xrange(0, p + 1)],
      # mimics test_vectors.py and takes a subset of all decimal types
      'pairwise' : all_pairs([(p, s) for p in xrange(1, 39) for s in xrange(0, p + 1)])
  }
  # We can cast for numerrics or string types.
  CAST_FROM = ['string', 'number']
  # Set the default precisin to 38 to operate on decimal values.
  getcontext().prec = 38
  # Represents a 0 in decimal
  DECIMAL_ZERO = Decimal('0')

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    cls.TestMatrix = TestMatrix()
    cls.TestMatrix.add_dimension(TestDimension('decimal_type',
        *TestDecimalCasting.DECIMAL_TYPES_MAP[cls.exploration_strategy()]))
    cls.TestMatrix.add_dimension(
        TestDimension('cast_from', *TestDecimalCasting.CAST_FROM))
    cls.iterations = 1

  def setup_method(self, method):
    self.max_bigint = int(self.execute_scalar("select max_bigint()"))

  def _gen_decimal_val(self, precision, scale):
    """Generates a Decimal object with the exact number of digits as the precision."""
    # Generates numeric string which has as many digits as the precision.
    num = str(randint(10**(precision - 1), int('9' * precision)))
    # Incorporate scale into the string.
    if scale != 0: num = "{0}.{1}".format(num[:-scale], num[precision - scale:])
    # Convert the generated decimal string into a Decimal object and return a -ive/+ive
    # version of it with equal probability.
    return Decimal(num) if randint(0,1) else Decimal("-{0}".format(num))

  def _assert_decimal_result(self, cast, actual, expected):
    assert actual == expected, "Cast: {0}, Expected: {1}, Actual: {2}".format(cast,\
        expected, actual)

  def _normalize_cast_expr(self, decimal_val, scale, from_string=False):
    """Convert the decimal value to a string litetal to avoid overflow.

    If an integer literal is greater than the max bigint supported by Impala, it
    overflows. This methods replaces it with a string literal.
    """
    if (scale == 0 and abs(decimal_val) > self.max_bigint) or from_string:
      return "select cast('{0}' as Decimal({1}, {2}))"
    return "select cast({0} as Decimal({1}, {2}))"

  def test_min_max_zero_null(self, vector):
    """Sanity test at limits.

    Verify that:
      - We can read decimal values at their +ive and -ive limits.
      - 0 is expressible in all decimal types.
      - NULL is expressible in all decimal types
    """
    precision, scale = vector.get_value('decimal_type')
    from_string = vector.get_value('cast_from') == 'string'
    dec_max = Decimal('{0}.{1}'.format('9' * (precision - scale), '9' * scale))
    # Multiplying large values eith -1 can produce an overflow.
    dec_min = Decimal('-{0}'.format(str(dec_max)))
    cast = self._normalize_cast_expr(dec_max, scale, from_string=from_string)
    # Test max
    res = Decimal(self.execute_scalar(cast.format(dec_max, precision, scale)))
    self._assert_decimal_result(cast, res, dec_max)
    # Test Min
    res = Decimal(self.execute_scalar(cast.format(dec_min, precision, scale)))
    self._assert_decimal_result(cast, res, dec_min)
    # Test zero
    res = Decimal(self.execute_scalar(cast.format(TestDecimalCasting.DECIMAL_ZERO,
      precision, scale)))
    self._assert_decimal_result(cast, res, TestDecimalCasting.DECIMAL_ZERO)
    # Test NULL
    null_cast = "select cast(NULL as Decimal({0}, {1}))".format(precision, scale)
    res = self.execute_scalar(null_cast)
    self._assert_decimal_result(null_cast, res, 'NULL')

  def test_exact(self, vector):
    """Test to verify that an exact representation of the desired Decimal type is
    maintained."""
    precision, scale = vector.get_value('decimal_type')
    from_string = vector.get_value('cast_from') == 'string'
    for i in xrange(self.iterations):
      val = self._gen_decimal_val(precision, scale)
      cast = self._normalize_cast_expr(val, scale, from_string=from_string)\
          .format(val, precision, scale)
      res = Decimal(self.execute_scalar(cast))
      self._assert_decimal_result(cast, res, val)

  def test_overflow(self, vector):
    """Test to verify that we always return NULL when trying to cast a number with greater
    precision that its intended decimal type"""
    precision, scale = vector.get_value('decimal_type')
    from_string = vector.get_value('cast_from') == 'string'
    for i in xrange(self.iterations):
      # Generate a decimal with a larger precision than the one we're casting to.
      val = self._gen_decimal_val(randint(precision + 1, 39), scale)
      cast = self._normalize_cast_expr(val, scale, from_string=from_string)\
          .format(val, precision, scale)
      res = self.execute_scalar(cast)
      self._assert_decimal_result(cast, res, 'NULL')

  def test_underflow(self, vector):
    """Test to verify that we truncate when the scale of the number being cast is higher
    than the target decimal type (with no change in precision).
    """
    precision, scale = vector.get_value('decimal_type')
    from_string = vector.get_value('cast_from') == 'string'
    if precision == scale:
      pytest.skip("Cannot underflow scale when precision and scale are equal")
    for i in xrange(self.iterations):
      new_scale = randint(scale + 1, precision)
      val = self._gen_decimal_val(precision, randint(new_scale, precision))
      # We don't need to normalize the cast expr because scale will never be zero
      cast = self._normalize_cast_expr(val, scale, from_string=from_string)\
          .format(val, precision, scale)
      res = Decimal(self.execute_scalar(cast))
      # Truncate the decimal value to its target scale with quantize.
      self._assert_decimal_result(cast, res, val.quantize(Decimal('0e-%s' % scale),
        rounding=ROUND_DOWN))
