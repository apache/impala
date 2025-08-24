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
from collections import OrderedDict
from itertools import product
from copy import deepcopy
import random
import logging
import os


LOG = logging.getLogger(__name__)
VECTOR = 'vector'
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

  def __repr__(self):
    return str(self)

  # Each value in a test vector is wrapped in the Value object. This wrapping is
  # done internally so this object should never need to be created by the user.
  class Value(object):
    def __init__(self, name, value):
      self.name = name
      self.value = value

    def __str__(self):
      return '"%s: %s"' % (self.name, self.value)

    def __repr__(self):
      return str(self)


# Matrix -> Collection of vectors
# Vector -> Call to get specific values
class ImpalaTestMatrix(object):
  def __init__(self, *args):
    self.dimensions = OrderedDict((arg.name, arg) for arg in args)
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
    # Pairwise fails if the number of inputs == 1. Use exhaustive in this case the
    # results will be the same.
    if len(self.dimensions) == 1:
      return self.__generate_exhaustive_combinations()
    vals = self.__extract_vector_values()
    all_vectors = [v for v in product(*vals) if self.is_valid(v)]
    # Add possibility to shuffle vectors to get different covering vector set.
    # This could excercise 3+ way combinations skipped by the pair wise algorithm.
    seed = os.environ.get('IMPALA_TEST_VECTOR_SEED', None)
    if seed:
      rnd = random.Random(seed)
      rnd.shuffle(all_vectors)
    found = set()
    result = []
    # Originally allpairspy was used for vector set generation to cover all pairs,
    # but it turned out to be unreliable when using constraints (IMPALA-13125). Below
    # is a simple implementation that may use more vectors than needed but will always
    # cover all parameter pairs if possible (constraints can lead to pairs that can't
    # be covered). A caveat is that first the product of all dimensions is needed
    # ('all_vectors'), which grows exponentially with the number of dimensions, but the
    # vector counts in Impala tests are still manageable.
    #
    # Note that there is not much to optimize in the test suites I checked, because due
    # to the constraints and the size differences between dimensions the vector set
    # is dictated by a few dimensions (file_format+exec_options) and this set can easily
    # cover pairs with other dimensions.
    #
    #  TODO: is there a nicer algorithm / is it needed?
    #   pair creation could be modelled as a bipartite graph with dimension pairs A and
    #   test vectors B as vertices, and a minimal set of B would be needed to have
    #   adjacent node for each vertex in A - I am pretty sure that this is polynomial, but
    #   didn't do the research
    # The following steps look for "better vectors" first that cover more dimension pairs
    # to avoid very suboptimal solutions - the example on allpairspy site was covered
    # with 25 vectors instead of the 21 of by allpairspy, which seems acceptable.
    remaining = self.__pairwise_step(all_vectors, result, found, len(self.dimensions))
    remaining = self.__pairwise_step(remaining, result, found, 2)
    remaining = self.__pairwise_step(remaining, result, found, 1)
    assert len(remaining) == 0

    res = [ImpalaTestVector(self.__deepcopy_vector_values(vec))
      for vec in result]
    return res

  def add_constraint(self, constraint_func):
    self.constraint_list.append(constraint_func)

  def clear_constraints(self):
    self.constraint_list = list()

  def __extract_vector_values(self):
    # The data is stored as a tuple of (name, [val1, val2, val3]). So extract the
    # actual values from this
    return [v[1] for v in self.dimensions.items()]

  def is_valid(self, vector):
    assert isinstance(vector, tuple)
    assert len(vector) == len(self.dimensions)
    for constraint in self.constraint_list:
      if not constraint(ImpalaTestVector(vector)):
        return False
    return True

  def __pairwise_step(self, vectors, results, pairs_covered, min_cover_count):
    """ Simple algorithm to select a subset of 'vectors' (each with N items) to cover
        every parameter pair.
        With 'min_cover_count'=1 all pairs will be covered (if possible), for bigger
        values only those vectors will be selected that cover at least 'min_cover_count'
        new pairs and the unused vectors will be returned in the result of the function.
        'pairs_covered' is a set of the already covered pairs. The key is a tuple of 4
        elements (i, v[i], j, v[j]) where i<j and both are < N. Each vector covers
        N * (N - 1) pairs.
    """
    remaining = []
    for v in vectors:
      cnt = 0
      for i, x in enumerate(v):
        for j, y in enumerate(v[i + 1:], i + 1):
          t = (i, x, j, y)
          if t not in pairs_covered:
            cnt += 1
      if cnt == 0: continue
      if cnt < min_cover_count:
        remaining.append(v)
        continue
      results.append(v)
      for i, x in enumerate(v):
        for j, y in enumerate(v[i + 1:], i + 1):
          t = (i, x, j, y)
          pairs_covered.add(t)
    return remaining
