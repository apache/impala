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

# A TextMatrix is used to generate a set of ImpalaTestVectors. The vectors that are
# generated are based on one or more ImpalaTestDimensions inputs. These lists define
# the set of values that are interesting to a test. For example, for file_format
# these might be 'seq', 'text', etc
#
# The ImpalaTestMatrix is then used to generate a set of ImpalaTestVectors. Each
# ImpalaTestVector contains a single value from each of the input ImpalaTestDimensions.
# An example:
#
# ImpalaTestMatrix.add_dimension('file_format', 'seq', 'text')
# ImpalaTestMatrix.add_dimension('agg_func', 'min', 'max', 'sum')
# ImpalaTestMatrix.add_dimension('col_type', 'int', 'bool')
# test_vectors = ImpalaTestMatrix.generate_test_vectors(...)
#
# Would return a collection of ImpalaTestVectors, with each one containing a
# combination of file_format, agg_func, and col_type:
# seq, min, int
# text, max, bool
# ...
#
# A ImpalaTestVector is an object itself, and the 'get_value' function is used to
# extract the actual value from the ImpalaTestVector for this particular combination:
# test_vector = test_vectors[0]
# print test_vector.get_value('file_format')
#
# The combinations of ImpalaTestVectors generated can be done in two ways: pairwise
# and exhaustive. Pairwise provides a way to get good coverage and reduce the total
# number of combinations generated where exhaustive will generate all valid
# combinations.
#
# Finally, the ImpalaTestMatrix also provides a way to add constraints to the vectors
# that are generated. This is useful to filter out invalid combinations. These can
# be added before calling 'generate_test_vectors'. The constraint is a function that
# accepts a ImpalaTestVector object and returns true if the vector is valid, false
# otherwise. For example, if we want to make sure 'bool' columns are not used with 'sum':
#
# ImpalaTestMatrix.add_constraint(lambda v:\
#    not (v.get_value('col_type') == 'bool' and v.get_value('agg_func') == 'sum'))
#
# Additional examples of usage can be found within the test suites.

from __future__ import absolute_import, division, print_function
from itertools import product
from copy import deepcopy
import logging


LOG = logging.getLogger(__name__)
# Literal constants to refer to some standard dimension names.
EXEC_OPTION = 'exec_option'
PROTOCOL = 'protocol'
TABLE_FORMAT = 'table_format'
# Literal constants to refer protocol names.
BEESWAX = 'beeswax'
HS2 = 'hs2'
HS2_HTTP = 'hs2-http'


def assert_exec_option_key(key):
  assert key.islower(), "Exec option " + key + " is not in lower case"


# A list of test dimension values.
class ImpalaTestDimension(list):
  def __init__(self, name, *args):
    self.name = name
    self.extend([ImpalaTestVector.Value(name, arg) for arg in args])


# A test vector that passed to test method. The ImpalaTestVector can be used to
# extract values for the specified dimension(s)
class ImpalaTestVector(object):
  def __init__(self, vector_values):
    self.vector_values = vector_values

  def get_value_with_default(self, name, default_value):
    for vector_value in self.vector_values:
      if vector_value.name == name:
        return vector_value.value
    return default_value

  def get_value(self, name):
    for vector_value in self.vector_values:
      if vector_value.name == name:
        return vector_value.value
    raise ValueError("Test vector does not contain value '%s'" % name)

  def get_protocol(self):
    return self.get_value(PROTOCOL)

  def get_table_format(self):
    return self.get_value(TABLE_FORMAT)

  def get_exec_option_dict(self):
    return self.get_value(EXEC_OPTION)

  def get_exec_option(self, option_name):
    value = self.get_value(EXEC_OPTION).get(option_name.lower())
    assert value is not None
    return value

  def set_exec_option(self, option_name, option_value):
    self.get_value(EXEC_OPTION)[option_name.lower()] = str(option_value)

  def unset_exec_option(self, option_name):
    del self.get_value(EXEC_OPTION)[option_name.lower()]

  def __str__(self):
      return ' | '.join(['%s' % vector_value for vector_value in self.vector_values])

  # Each value in a test vector is wrapped in the Value object. This wrapping is
  # done internally so this object should never need to be created by the user.
  class Value(object):
    def __init__(self, name, value):
      self.name = name
      self.value = value

    def __str__(self):
      return '%s: %s' % (self.name, self.value)


# Matrix -> Collection of vectors
# Vector -> Call to get specific values
class ImpalaTestMatrix(object):
  def __init__(self, *args):
    self.dimensions = dict((arg.name, arg) for arg in args)
    self.constraint_list = list()
    self.independent_exec_option_names = set()

  def add_dimension(self, dimension):
    self.dimensions[dimension.name] = dimension
    if dimension.name == EXEC_OPTION:
      for name in list(self.independent_exec_option_names):
        LOG.warn("Reassigning {} dimension will remove exec option {}={} that was "
            "independently declared through add_exec_option_dimension.".format(
              EXEC_OPTION, name, [v.value for v in self.dimensions[name]]))
        self.clear_dimension(name)

  def assert_unique_exec_option_key(self, key):
    """Assert that 'exec_option' dimension exist and 'key' is not exist yet
    in self.dimension['exec_option']."""
    assert_exec_option_key(key)
    assert EXEC_OPTION in self.dimensions, (
        "Must have '" + EXEC_OPTION + "' dimension previously declared!")

    for vector in self.dimensions[EXEC_OPTION]:
      assert key not in vector.value, (
          "'{}' is already exist in '{}' dimension!".format(key, EXEC_OPTION))

    # 'key' must not previously declared with add_exec_option_dimension().
    assert key not in self.independent_exec_option_names, (
        "['{}']['{}'] was previously declared with non-unique value: {}".format(
          EXEC_OPTION, key, [dim.value for dim in self.dimensions[key]]))

  def add_mandatory_exec_option(self, key, value):
    """Append key=value pair into 'exec_option' dimension."""
    self.assert_unique_exec_option_key(key)

    for vector in self.dimensions[EXEC_OPTION]:
      vector.value[key] = value

  def add_exec_option_dimension(self, dimension):
    """
    Add 'dimension' into 'self.dimensions' and memorize the name.
    During vector generation, all dimensions registered through this method will be
    embedded into 'exec_option' dimension. This is intended to maintain pairwise and
    exhaustive combination correct while eliminating the need for individual test method
    to append the custom exec options into 'exec_option' dimension themself.
    """
    self.assert_unique_exec_option_key(dimension.name)
    assert len(dimension) > 1, (
        "Dimension " + dimension.name + " must have more than 1 possible value! "
        "Use add_mandatory_exec_option() instead.")

    self.add_dimension(dimension)
    self.independent_exec_option_names.add(dimension.name)

  def clear(self):
    self.dimensions.clear()
    self.independent_exec_option_names = set()

  def clear_dimension(self, dimension_name):
    del self.dimensions[dimension_name]
    self.independent_exec_option_names.discard(dimension_name)

  def has_dimension(self, dimension_name):
    return dimension_name in self.dimensions

  def generate_test_vectors(self, exploration_strategy):
    if not self.dimensions:
      return list()
    # TODO: Check valid exploration strategies, provide more options for exploration
    if exploration_strategy == 'exhaustive':
      return self.__generate_exhaustive_combinations()
    elif exploration_strategy in ['core', 'pairwise']:
      return self.__generate_pairwise_combinations()
    else:
      raise ValueError('Unknown exploration strategy: %s' % exploration_strategy)

  def __deepcopy_vector_values(self, vector_values):
    """Return a deepcopy of vector_values and merge exec options declared through
    add_exec_option_dimension() into 'exec_option' dimension."""
    values = []
    exec_values = []
    exec_option = None
    for val in vector_values:
      if val.name == EXEC_OPTION:
        # 'exec_option' is a map. We need to deepcopy the value for each vector.
        exec_option = deepcopy(val.value)
      elif val.name in self.independent_exec_option_names:
        # save this to merge into exec_option later.
        exec_values.append(val)
      else:
        values.append(val)
    if self.independent_exec_option_names:
      assert exec_option is not None, (
        "Must have '" + EXEC_OPTION + "' dimension previously declared!")
      for val in exec_values:
        exec_option[val.name] = val.value
    if exec_option:
      values.append(ImpalaTestVector.Value(EXEC_OPTION, exec_option))
    return values

  def __generate_exhaustive_combinations(self):
    return [ImpalaTestVector(self.__deepcopy_vector_values(vec))
      for vec in product(*self.__extract_vector_values()) if self.is_valid(vec)]

  def __generate_pairwise_combinations(self):
    from allpairspy import AllPairs
    all_pairs = AllPairs

    # Pairwise fails if the number of inputs == 1. Use exhaustive in this case the
    # results will be the same.
    if len(self.dimensions) == 1:
      return self.__generate_exhaustive_combinations()
    return [ImpalaTestVector(self.__deepcopy_vector_values(vec))
      for vec in all_pairs(self.__extract_vector_values(), filter_func=self.is_valid)]

  def add_constraint(self, constraint_func):
    self.constraint_list.append(constraint_func)

  def clear_constraints(self):
    self.constraint_list = list()

  def __extract_vector_values(self):
    # The data is stored as a tuple of (name, [val1, val2, val3]). So extract the
    # actual values from this
    return [v[1] for v in self.dimensions.items()]

  def is_valid(self, vector):
    for constraint in self.constraint_list:
      if (isinstance(vector, list) or isinstance(vector, tuple)) and\
          len(vector) == len(self.dimensions):
        valid = constraint(ImpalaTestVector(vector))
        if valid:
          continue
        return False
    return True
