# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# A TextMatrix is used to generate a set of TestVectors. The vectors that are generated
# are based on one or more TestDimensions inputs. These lists define the set of
# values that are interesting to a test. For example, for file_format these might be
# 'seq', 'text', etc
#
# The TestMatrix is then used to generate a set of TestVectors. Each TestVector contains a
# single value from each of the input TestDimensions. An example:
#
# TestMatrix.add_dimension('file_format', 'seq', 'text')
# TestMatrix.add_dimension('agg_func', 'min', 'max', 'sum')
# TestMatrix.add_dimension('col_type', 'int', 'bool')
# test_vectors = TestMatrix.generate_test_vectors(...)
#
# Would return a collection of TestVectors, with each one containing a combination of
# file_format, agg_func, and col_type:
# seq, min, int
# text, max, bool
# ...
#
# A TestVector is an object itself, and the 'get_value' function is used to extract the
# actual value from the TestVector for this particular combination:
# test_vector = test_vectors[0]
# print test_vector.get_value('file_format')
#
# The combinations of TestVectors generated can be done in two ways: pairwise and
# exhaustive. Pairwise provides a way to get good coverage and reduce the total number
# of combinations generated where exhaustive will generate all valid combinations.
#
# Finally, the TestMatrix also provides a way to add constraints to the vectors that
# are generated. This is useful to filter out invalid combinations. These can be added
# before calling 'generate_test_vectors'. The constraint is a function that
# accepts a TestVector object and returns true if the vector is valid, false otherwise.
# For example, if we want to make sure 'bool' columns are not used with 'sum':
#
# TestMatrix.add_constraint(lambda v:\
#    not (v.get_value('col_type') == 'bool and v.get_value('agg_func') == 'sum'))
#
# Additional examples of usage can be found within the test suites.
import collections
import logging
import os
from itertools import product

# A list of test dimension values.
class TestDimension(list):
  def __init__(self, name, *args):
    self.name = name
    self.extend([TestVector.Value(name, arg) for arg in args])


# A test vector that passed to test method. The TestVector can be used to extract values
# for the specified dimension(s)
class TestVector(object):
  def __init__(self, vector_values):
    self.vector_values = vector_values

  def get_value(self, name):
    return next(vector_value for vector_value in self.vector_values\
                                              if vector_value.name == name).value

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
class TestMatrix(object):
  def __init__(self, *args):
    self.dimensions = dict((arg.name, arg) for arg in args)
    self.constraint_list = list()

  def add_dimension(self, dimension):
    self.dimensions[dimension.name] = dimension

  def clear(self):
    self.dimensions.clear()

  def clear_dimension(self, dimension_name):
    del self.dimensions[dimension_name]

  def has_dimension(self, dimension_name):
    return self.dimensions.has_key(dimension_name)

  def generate_test_vectors(self, exploration_strategy):
    if not self.dimensions:
      return list()
    # TODO: Check valid exploration strategies, provide more options for exploration
    if exploration_strategy == 'exhaustive':
      return self.__generate_exhaustive_combinations()
    elif exploration_strategy in ['core', 'pairwise']:
      return self.__generate_pairwise_combinations()
    else:
      raise ValueError, 'Unknown exploration strategy: %s' % exploration_strategy

  def __generate_exhaustive_combinations(self):
    return [TestVector(vec) for vec in product(*self.__extract_vector_values()) if\
                                               self.is_valid(vec)]

  def __generate_pairwise_combinations(self):
    import metacomm.combinatorics.all_pairs2
    all_pairs = metacomm.combinatorics.all_pairs2.all_pairs2

    # Pairwise fails if the number of inputs == 1. Use exhaustive in this case the
    # results will be the same.
    if len(self.dimensions) == 1:
      return self.__generate_exhaustive_combinations()
    return [TestVector(vec) for vec in all_pairs(self.__extract_vector_values(),
                                                 filter_func = self.is_valid)]

  def add_constraint(self, constraint_func):
    self.constraint_list.append(constraint_func)

  def clear_constraints(self):
    self.constraint_list = list()

  def __extract_vector_values(self):
    # The data is stored as a tuple of (name, [val1, val2, val3]). So extract the actual
    # values from this
    return [v[1] for v in self.dimensions.items()]

  def is_valid(self, vector):
    for constraint in self.constraint_list:
      if (isinstance(vector, list) or isinstance(vector, tuple)) and\
          len(vector) == len(self.dimensions):
        valid = constraint(TestVector(vector))
        if valid:
          continue
        return False
    return True
