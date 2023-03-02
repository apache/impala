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
from datetime import datetime, timedelta
from decimal import Decimal as PyDecimal
from random import randint, random, uniform

from tests.comparison.db_types import Boolean, Char, Decimal, Float, Int, Timestamp

class RandomValGenerator(object):

  def __init__(self,
      min_number=-1000,
      max_number=1000,
      min_date=datetime(1990, 1, 1),
      max_date=datetime(2030, 1, 1),
      null_val_percentage=0.1):
    if type(min_number) != int or type(max_number) != int:
      raise Exception("min_number and max_number must be integers but were %s and %s"
          % (type(min_number), type(max_number)))
    self.min_number = min_number
    self.max_number = max_number
    self.min_date = min_date
    self.max_date = max_date
    self.null_val_percentage = null_val_percentage

  def generate_val(self, val_type):
    '''Generate and return a single random val. Use the val_type parameter to
       specify the type of val to generate. See types.py for valid val_type
       options.

       Ex:
         generator = RandomValGenerator(min_number=1, max_number=5)
         val = generator.generate_val(model.Int)
         assert 1 <= val and val <= 5
    '''
    return next(self.create_val_generator(val_type))

  def create_val_generator(self, val_type):
    '''Generate and return a single random val. Use the val_type parameter to
       specify the type of val to generate. See types.py for valid val_type
       options.

       Ex:
         generator = RandomValGenerator(min_number=1, max_number=5)
         val = generator.generate_val(model.Int)
         assert 1 <= val and val <= 5
    '''
    if issubclass(val_type, Int):
      def val():
        return randint(
            max(self.min_number, val_type.MIN), min(val_type.MAX, self.max_number))
    elif issubclass(val_type, Char):
      def val():
        val = randint(
            max(self.min_number, val_type.MIN), min(val_type.MAX, self.max_number))
        return None if val is None else str(val)[:val_type.MAX]
    elif issubclass(val_type, Decimal):
      # Create an int within the maximum length of the Decimal, then shift the decimal
      # point as needed.
      max_digits = val_type.MAX_DIGITS
      fractal_digits = val_type.MAX_FRACTIONAL_DIGITS
      max_type_val = 10 ** max_digits
      decimal_point_shift = 10 ** fractal_digits
      max_val = min(self.max_number * decimal_point_shift, max_type_val - 1)
      min_val = max(self.min_number * decimal_point_shift, -1 * max_type_val + 1)
      def val():
        val = PyDecimal(randint(min_val, max_val))
        return val.scaleb(-1 * val_type.MAX_FRACTIONAL_DIGITS)
    elif issubclass(val_type, Float):
      def val():
        return uniform(self.min_number, self.max_number)
    elif issubclass(val_type, Timestamp):
      delta = self.max_date - self.min_date
      delta_in_seconds = delta.days * 24 * 60 * 60 + delta.seconds
      def val():
        offset_in_seconds = randint(0, delta_in_seconds)
        val = self.min_date + timedelta(0, offset_in_seconds)
        return datetime(val.year, val.month, val.day)
    elif issubclass(val_type, Boolean):
      def val():
        return randint(0, 1) == 1
    else:
      raise Exception('Unsupported type %s' % val_type.__name__)

    while True:
      if random() < self.null_val_percentage:
        yield None
      else:
        yield val()
