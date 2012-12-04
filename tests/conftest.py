#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# py.test configuration module
#
import logging
logging.basicConfig(level=logging.INFO, format='%(threadName)s: %(message)s')
LOG = logging.getLogger('test_configuration')

def pytest_addoption(parser):
  """Adds a new command line options to py.test"""
  parser.addoption("--exploration_strategy", default="core", help=\
                   "Exploration strategy for tests - core, pairwise, or exhaustive.")

  parser.addoption("--impalad", default="localhost:21000", help=\
                   "The impalad host:port to run tests against.")

  parser.addoption("--hive_server", default="localhost:10000", help=\
                   "The hive server host:port to connect to.")

  parser.addoption("--update_results", action="store_true", default=False, help=\
                   "If set, will generate new results for all tests run instead of "\
                   "verifying the results.")

  parser.addoption("--file_format_filter", default=None, help=\
                   "Run tests only using the specified file format")

def pytest_generate_tests(metafunc):
  """
  This is a hook to parameterize the tests based on the input vector.

  If a test has the 'vector' fixture specified, this code is invoked and it will build
  a set of test vectors to parameterize the test with.
  """
  if 'vector' in metafunc.fixturenames:
    metafunc.cls.add_test_dimensions()
    if metafunc.config.option.file_format_filter:
      file_formats = metafunc.config.option.file_format_filter.split(',')
      metafunc.cls.TestMatrix.add_constraint(lambda v:\
          v.get_value('table_format').file_format in file_formats)
    vectors = metafunc.cls.TestMatrix.generate_test_vectors(
        metafunc.config.option.exploration_strategy)
    if len(vectors) == 0:
      LOG.warning('No test vectors generated. Check constraints and input vectors')

    vector_names = ['%s' % vector for vector in vectors]
    # In the case this is a test result update, just select a single test vector to run
    # the results are expected to be the same in all cases
    if metafunc.config.option.update_results:
      vectors = vectors[0:1]
      vector_names = vector_names[0:1]
    metafunc.parametrize('vector', vectors, ids=vector_names)
