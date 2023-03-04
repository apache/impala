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

# Generates random decimal numbers and verifies that mathematical
# operations return correct results under decimal_v2.

from __future__ import absolute_import, division, print_function
from builtins import range
import decimal
import math
import pytest
import random

from tests.beeswax.impala_beeswax import ImpalaBeeswaxException
from tests.common.impala_test_suite import ImpalaTestSuite
from tests.common.test_dimensions import create_single_exec_option_dimension
from tests.common.test_vector import ImpalaTestDimension, ImpalaTestMatrix

class TestDecimalFuzz(ImpalaTestSuite):

  # Impala's max precision for decimals is 38, so we should have the same in the tests
  decimal.getcontext().prec = 38

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def add_test_dimensions(cls):
    cls.ImpalaTestMatrix = ImpalaTestMatrix()
    cls.ImpalaTestMatrix.add_dimension(create_single_exec_option_dimension())
    cls.iterations = 10000

  def weighted_choice(self, options):
    total_weight = sum(options.values())
    numeric_choice = random.uniform(0, total_weight)
    last_choice = None
    for choice, weight in options.items():
      if numeric_choice <= weight:
        return choice
      numeric_choice -= weight
      if weight > 0:
        last_choice = choice
    return last_choice

  def get_decimal(self):
    '''Returns a 3-tuple with string values of (value, precision, scale). The function
    does not always return completely random values, we try to bias it to select
    more interesting values.'''

    def random_precision():
      return random.randint(1, 38)

    def extreme_precision():
      return 38

    precision_weights = {}
    precision_weights[random_precision] = 0.8
    precision_weights[extreme_precision] = 0.2
    precision = self.weighted_choice(precision_weights)()

    def random_scale(precision):
      return random.randint(0, precision)

    def extreme_scale(precision):
      return random.choice([0, precision])

    scale_weights = {}
    scale_weights[random_scale] = 0.9
    scale_weights[extreme_scale] = 0.1
    scale = self.weighted_choice(scale_weights)(precision)

    def random_value(precision):
      '''Generates a completely random value.'''

      def num_digits_random(precision):
        return random.randint(1, precision)

      def num_digits_all(precision):
        return precision

      # Determine how many digits the value is going to have.
      num_digits_weights = {}
      num_digits_weights[num_digits_random] = 0.8
      num_digits_weights[num_digits_all] = 0.2
      num_digits = self.weighted_choice(num_digits_weights)(precision)

      no_zero = '123456789'
      with_zero = '0123456789'
      result = random.choice(no_zero)
      for _ in range(num_digits - 1):
        result += random.choice(with_zero)

      return result

    def special_case_binary_value(precision):
      '''Generates a value that looks like 11111... or 10000... in binary number
      system.'''

      def exponent_random(precision):
        return random.randint(0, int(precision * math.log(10, 2)))

      def exponent_max(precision):
        return int(precision * math.log(10, 2))

      exponent_weights = {}
      exponent_weights[exponent_random] = 0.8
      exponent_weights[exponent_max] = 0.2
      exponent = self.weighted_choice(exponent_weights)(precision)

      value = 2 ** exponent
      if random.random() < 0.5:
        value -= 1
      return '{0}'.format(value)

    def special_case_decimal_value(precision):
      '''Generates a value that looks like 99999... or 10000... in decimal number
      system.'''

      def num_digits_random(precision):
        return random.randint(1, precision)

      def num_digits_max(precision):
        return precision

      num_digits_weights = {}
      num_digits_weights[num_digits_random] = 8
      num_digits_weights[num_digits_max] = 0.2
      num_digits = self.weighted_choice(num_digits_weights)(precision)

      value = 10 ** num_digits

      if num_digits == precision or random.random() < 0.5:
        value -= 1

      return '{0}'.format(value)

    value_weights = {}
    value_weights[random_value] = 0.6
    value_weights[special_case_binary_value] = 0.2
    value_weights[special_case_decimal_value] = 0.2

    value = self.weighted_choice(value_weights)(precision)

    # Randomly determine the placement of the decimal mark.
    # The smallest index where the decimal mark can be placed in the number string.
    min_dot_location = max(len(value) - scale, 0)
    # The largest index where the decimal mark can be placed in the number string.
    max_dot_location = min(precision - scale, len(value))
    dot_location = random.randint(min_dot_location, max_dot_location)

    if dot_location == 0:
      value = '0.' + value
    elif dot_location == len(value):
      pass
    else:
      value = value[:dot_location] + '.' + value[dot_location:]

    if random.random() < 0.5:
      # Negate the number.
      value = '-' + value
    return (value, precision, scale)

  def result_equals(self, expected, actual):
    '''Verify that the expected result is equal to the actual result. We verify equality
    by rounding the expected result to different numbers of places and verifying that the
    actual result is matched in at least one of the cases.'''
    if actual == expected:
      return True

    if actual is None:
      # Overflow
      if abs(expected) > decimal.Decimal("9" * 32):
        # If the expected result is larger than 10^32 - 1, it's not unreasonable for
        # there to be an overflow in Impala because the minimum scale is 6 and
        # 38 (max precision) - 6 = 32.
        return True
      return False

    for num_digits_after_dot in range(39):
      # Reduce the number of digits after the dot in the expected_result to different
      # amounts. If it matches the actual result in at least one of the cases, we
      # consider the actual result to be acceptable.
      truncated_expected = expected.quantize(
          decimal.Decimal("1e-{0}".format(num_digits_after_dot)),
          rounding=decimal.ROUND_HALF_UP)
      if actual == truncated_expected:
        return True
    return False

  def execute_one_decimal_op(self):
    '''Executes a single query and compares the result to a result that we computed in
    Python.'''
    op = random.choice(['+', '-', '*', '/', '%'])
    value1, precision1, scale1 = self.get_decimal()
    value2, precision2, scale2 = self.get_decimal()

    query = ('select cast({value1} as decimal({precision1},{scale1})) {op} '
        'cast({value2} as decimal({precision2},{scale2}))').format(op=op,
        value1=value1, precision1=precision1, scale1=scale1,
        value2=value2, precision2=precision2, scale2=scale2)

    try:
      result = self.execute_scalar(query, query_options={'decimal_v2': 'true'})
    except ImpalaBeeswaxException as e:
      result = None
    if result is not None:
      result = decimal.Decimal(result)

    with decimal.localcontext() as ctx:
      # Set the decimal context to a large precision initially, so that the
      # mathematical operations are performed at a high precision.
      ctx.prec = 80

      try:
        if op == '+':
          expected_result = decimal.Decimal(value1) + decimal.Decimal(value2)
        elif op == '-':
          expected_result = decimal.Decimal(value1) - decimal.Decimal(value2)
        elif op == '*':
          expected_result = decimal.Decimal(value1) * decimal.Decimal(value2)
        elif op == '/':
          expected_result = decimal.Decimal(value1) / decimal.Decimal(value2)
        elif op == '%':
          expected_result = decimal.Decimal(value1) % decimal.Decimal(value2)
        else:
          assert False
      except decimal.InvalidOperation as e:
        expected_result = None
      except decimal.DivisionByZero as e:
        expected_result = None
      assert self.result_equals(expected_result, result)

  def test_decimal_ops(self, vector):
    for _ in range(self.iterations):
      self.execute_one_decimal_op()

  def width_bucket(self, val, min_range, max_range, num_buckets):
    # Multiplying the values by 10**40 guarantees that the numbers can be converted
    # to int without losing information.
    val_int = int(decimal.Decimal(val) * 10**40)
    min_range_int = int(decimal.Decimal(min_range) * 10**40)
    max_range_int = int(decimal.Decimal(max_range) * 10**40)

    if min_range_int >= max_range_int:
      return None
    if val_int < min_range_int:
      return 0
    if val_int > max_range_int:
      return num_buckets + 1

    range_size = max_range_int - min_range_int
    dist_from_min = val_int - min_range_int
    return (num_buckets * dist_from_min) // range_size + 1

  def execute_one_width_bucket(self):
    val, val_prec, val_scale = self.get_decimal()
    min_range, min_range_prec, min_range_scale = self.get_decimal()
    max_range, max_range_prec, max_range_scale = self.get_decimal()
    num_buckets = random.randint(1, 2147483647)

    query = ('select width_bucket('
        'cast({val} as decimal({val_prec},{val_scale})), '
        'cast({min_range} as decimal({min_range_prec},{min_range_scale})), '
        'cast({max_range} as decimal({max_range_prec},{max_range_scale})), '
        '{num_buckets})')

    query = query.format(val=val, val_prec=val_prec, val_scale=val_scale,
        min_range=min_range, min_range_prec=min_range_prec,
        min_range_scale=min_range_scale,
        max_range=max_range, max_range_prec=max_range_prec,
        max_range_scale=max_range_scale,
        num_buckets=num_buckets)

    expected_result = self.width_bucket(val, min_range, max_range, num_buckets)
    if not expected_result:
      return

    try:
      result = self.execute_scalar(query, query_options={'decimal_v2': 'true'})
      assert int(result) == expected_result
    except ImpalaBeeswaxException as e:
      if "You need to wrap the arguments in a CAST" not in str(e):
        # Sometimes the decimal inputs are incompatible with each other, so it's ok
        # to ignore this error.
        raise e

  def test_width_bucket(self, vector):
    for _ in range(self.iterations):
      self.execute_one_width_bucket()
