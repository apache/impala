#!/usr/bin/env python
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
# py.test configuration module
#
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
