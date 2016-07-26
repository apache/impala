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

import logging
import os
import re

try:
  from elftools.elf.elffile import ELFFile
except ImportError as e:
  # Handle pre-python2.7s' lack of collections.OrderedDict, which we include in
  # impala-python as ordereddict.OrderedDict.
  if 'cannot import name OrderedDict' == str(e):
    import monkeypatch
    from ordereddict import OrderedDict
    monkeypatch.patch(OrderedDict, 'collections', 'OrderedDict')
    from elftools.elf.elffile import ELFFile
  else:
    raise e


LOG = logging.getLogger('tests.common.environ')


# See if Impala is running with legacy aggregations and/or hash joins. This is kind of a
# hack. It would be better to poll Impala whether it is doing so.
test_start_cluster_args = os.environ.get("TEST_START_CLUSTER_ARGS", "")
old_agg_regex = "enable_partitioned_aggregation=false"
old_hash_join_regex = "enable_partitioned_hash_join=false"
USING_OLD_AGGS_JOINS = re.search(old_agg_regex, test_start_cluster_args) is not None or \
    re.search(old_hash_join_regex, test_start_cluster_args) is not None

# Find the likely BuildType of the running Impala. Assume it's found through the path
# $IMPALA_HOME/be/build/latest as a fallback.
impala_home = os.environ.get("IMPALA_HOME", "")
build_type_arg_regex = re.compile(r'--build_type=(\w+)', re.I)
build_type_arg_search_result = re.search(build_type_arg_regex, test_start_cluster_args)

if build_type_arg_search_result is not None:
  build_type_dir = build_type_search_result.groups()[0].lower()
else:
  build_type_dir = 'latest'

# Resolve any symlinks in the path.
impalad_basedir = \
    os.path.realpath(os.path.join(impala_home, 'be/build', build_type_dir)).rstrip('/')

IMPALAD_PATH = os.path.join(impalad_basedir, 'service', 'impalad')


class SpecificImpaladBuildTypes:
  """
  Represent a specific build type. In reality, there 5 specific build types. These
  specific build types are needed by Python test code.

  The specific build types and their *most distinguishing* compiler options are:

  1. ADDRESS_SANITIZER (clang -fsanitize=address)
  2. DEBUG (gcc -ggdb)
  3. DEBUG_CODE_COVERAGE (gcc -ggdb -ftest-coverage)
  4. RELEASE (gcc)
  5. RELEASE_CODE_COVERAGE (gcc -ftest-coverage)
  """
  # ./buildall.sh -asan
  ADDRESS_SANITIZER = 'address_sanitizer'
  # ./buildall.sh
  DEBUG = 'debug'
  # ./buildall.sh -codecoverage
  DEBUG_CODE_COVERAGE = 'debug_code_coverage'
  # ./buildall.sh -release
  RELEASE = 'release'
  # ./buildall.sh -release -codecoverage
  RELEASE_CODE_COVERAGE = 'release_code_coverage'


class ImpaladBuild(object):
  """
  Acquires and provides characteristics about the way the Impala under test was compiled
  and its likely effects on its responsiveness to automated test timings.
  """
  def __init__(self, impalad_path):
    self.impalad_path = impalad_path
    die_name, die_producer = self._get_impalad_dwarf_info()
    self._set_impalad_build_type(die_name, die_producer)

  @property
  def specific_build_type(self):
    """
    Return the correct SpecificImpaladBuildTypes for the Impala under test.
    """
    return self._specific_build_type

  def has_code_coverage(self):
    """
    Return whether the Impala under test was compiled with code coverage enabled.
    """
    return self.specific_build_type in (SpecificImpaladBuildTypes.DEBUG_CODE_COVERAGE,
                                        SpecificImpaladBuildTypes.RELEASE_CODE_COVERAGE)

  def is_asan(self):
    """
    Return whether the Impala under test was compiled with ASAN.
    """
    return self.specific_build_type == SpecificImpaladBuildTypes.ADDRESS_SANITIZER

  def is_dev(self):
    """
    Return whether the Impala under test is a development build (i.e., any debug or ASAN
    build).
    """
    return self.specific_build_type in (
        SpecificImpaladBuildTypes.ADDRESS_SANITIZER, SpecificImpaladBuildTypes.DEBUG,
        SpecificImpaladBuildTypes.DEBUG_CODE_COVERAGE)

  def runs_slowly(self):
    """
    Return whether the Impala under test "runs slowly". For our purposes this means
    either compiled with code coverage enabled or ASAN.
    """
    return self.has_code_coverage() or self.is_asan()

  def _get_impalad_dwarf_info(self):
    """
    Read the impalad_path ELF binary, which is supposed to contain DWARF, and read the
    DWARF to understand the compiler options. Return a 2-tuple of the two useful DIE
    attributes of the first compile unit: the DW_AT_name and DW_AT_producer. If
    something goes wrong doing this, log a warning and return nothing.
    """
    # Some useful references:
    # - be/CMakeLists.txt
    # - gcc(1), especially -grecord-gcc-switches, -g, -ggdb, -gdwarf-2
    # - readelf(1)
    # - general reading about DWARF
    # A useful command for exploration without having to wade through many bytes is:
    # readelf --debug-dump=info --dwarf-depth=1 impalad
    # The DWARF lines are long, raw, and nasty; I'm hesitant to paste them here, so
    # curious readers are highly encouraged to try the above, or read IMPALA-3501.
    die_name = None
    die_producer = None
    try:
      with open(self.impalad_path, 'rb') as fh:
        impalad_elf = ELFFile(fh)
        if impalad_elf.has_dwarf_info():
          dwarf_info = impalad_elf.get_dwarf_info()
          # We only need the first CU, hence the unconventional use of the iterator
          # protocol.
          cu_iterator = dwarf_info.iter_CUs()
          first_cu = next(cu_iterator)
          top_die = first_cu.get_top_DIE()
          die_name = top_die.attributes['DW_AT_name'].value
          die_producer = top_die.attributes['DW_AT_producer'].value
    except Exception as e:
      LOG.warn('Failure to read DWARF info from {0}: {1}'.format(self.impalad_path,
                                                                 str(e)))
    return die_name, die_producer

  def _set_impalad_build_type(self, die_name, die_producer):
    """
    Use a heuristic based on the DW_AT_producer and DW_AT_name of the first compile
    unit, as returned by _get_impalad_dwarf_info(), to figure out which of 5 supported
    builds of impalad we're dealing with. If the heuristic can't determine, fall back to
    assuming a debug build and log a warning.
    """
    ASAN_CU_NAME = 'asan_preinit.cc'
    NON_ASAN_CU_NAME = 'daemon-main.cc'
    GDB_FLAG = '-ggdb'
    CODE_COVERAGE_FLAG = '-ftest-coverage'

    if die_name is None or die_producer is None:
      LOG.warn('Not enough DWARF info in {0} to determine build type; choosing '
               'DEBUG'.format(self.impalad_path))
      self._specific_build_type = SpecificImpaladBuildTypes.DEBUG
      return

    is_debug = GDB_FLAG in die_producer
    specific_build_type = SpecificImpaladBuildTypes.DEBUG

    if die_name.endswith(ASAN_CU_NAME):
      specific_build_type = SpecificImpaladBuildTypes.ADDRESS_SANITIZER
    elif not die_name.endswith(NON_ASAN_CU_NAME):
      LOG.warn('Unexpected DW_AT_name in first CU: {0}; choosing '
               'DEBUG'.format(die_name))
      specific_build_type = SpecificImpaladBuildTypes.DEBUG
    elif CODE_COVERAGE_FLAG in die_producer:
      if is_debug:
        specific_build_type = SpecificImpaladBuildTypes.DEBUG_CODE_COVERAGE
      else:
        specific_build_type = SpecificImpaladBuildTypes.RELEASE_CODE_COVERAGE
    else:
      if is_debug:
        specific_build_type = SpecificImpaladBuildTypes.DEBUG
      else:
        specific_build_type = SpecificImpaladBuildTypes.RELEASE

    self._specific_build_type = specific_build_type


IMPALAD_BUILD = ImpaladBuild(IMPALAD_PATH)


def specific_build_type_timeout(
    default_timeout, slow_build_timeout=None, asan_build_timeout=None,
    code_coverage_build_timeout=None):
  """
  Return a test environment-specific timeout based on the sort of
  SpecificImpalaBuildType under test.

  Required parameter: default_timeout - default timeout value. This applies when Impala is
  a standard release or debug build, or if no other timeouts are specified.

  Optional parameters:
  slow_build_timeout - timeout to use if we're running against *any* build known to be
  slow. If specified, this will preempt default_timeout if Impala is expected to be
  "slow". You can use this as a shorthand in lieu of specifying all of the following
  parameters.

  The parameters below correspond to specific build types. These preempt both
  slow_build_timeout and default_timeout, if the Impala under test is a build of the
  applicable type:

  asan_build_timeout - timeout to use if Impala with ASAN is running

  code_coverage_build_timeout - timeout to use if Impala with code coverage is running
  (both debug and release code coverage)
  """

  if IMPALAD_BUILD.is_asan() and asan_build_timeout is not None:
    timeout_val = asan_build_timeout
  elif IMPALAD_BUILD.has_code_coverage() and code_coverage_build_timeout is not None:
    timeout_val = code_coverage_build_timeout
  elif IMPALAD_BUILD.runs_slowly() and slow_build_timeout is not None:
    timeout_val = slow_build_timeout
  else:
    timeout_val = default_timeout
  return timeout_val
