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
#
# Utility functions for calculating common mathematical measurements. Note that although
# some of these functions are available in external python packages (ex. numpy), these
# are simple enough that it is better to implement them ourselves to avoid extra
# dependencies.

from __future__ import absolute_import, division, print_function
from builtins import range
import math
import random
import string

def calculate_avg(values):
  return sum(values) / float(len(values))

def calculate_stddev(values):
  """Return the standard deviation of a numeric iterable."""
  avg = calculate_avg(values)
  return math.sqrt(calculate_avg([(val - avg)**2 for val in values]))

def calculate_median(values):
  """Return the median of a numeric iterable."""
  if all([v is None for v in values]): return None
  sorted_values = sorted(values)
  length = len(sorted_values)
  if length % 2 == 0:
    return (sorted_values[length // 2] + sorted_values[length // 2 - 1]) / 2
  else:
    return sorted_values[length // 2]

def calculate_geomean(values):
  """ Calculates the geometric mean of the given collection of numerics """
  if len(values) > 0:
    product = 1.0
    exponent = 1.0 / len(values)
    for value in values:
      product *= value ** exponent
    return product

def calculate_tval(avg, stddev, iters, ref_avg, ref_stddev, ref_iters):
  """
  Calculates the t-test t value for the given result and refrence.

  Uses the Welch's t-test formula. For more information see:
  http://en.wikipedia.org/wiki/Student%27s_t-distribution#Table_of_selected_values
  http://en.wikipedia.org/wiki/Student's_t-test
  """
  # SEM (standard error mean) = sqrt(var1/N1 + var2/N2)
  # t = (X1 - X2) / SEM
  sem = math.sqrt((math.pow(stddev, 2) / iters) + (math.pow(ref_stddev, 2) / ref_iters))
  return (avg - ref_avg) / sem

def get_random_id(length):
  return ''.join(
      random.choice(string.ascii_uppercase + string.digits) for _ in range(length))


def calculate_mwu(samples, ref_samples):
  """
  Calculates the Mann-Whitney U Test Z value for the given samples and reference.
  """
  tag_a = [(s, 'A') for s in samples]
  tab_b = [(s, 'B') for s in ref_samples]
  ab = tag_a + tab_b
  ab.sort()
  # Assume no ties
  u = 0
  count_b = 0
  for v in ab:
    if v[1] == 'A':
      u += count_b
    else:
      count_b += 1
  # u is normally distributed with the following mean and standard deviation:
  mean = len(samples) * len(ref_samples) / 2.0
  stddev = math.sqrt(len(samples) * len(ref_samples) * (1 + len(ab)) / 12.0)
  return (u - mean) / stddev
