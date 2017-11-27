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

LOG = logging.getLogger('tests.common.environ')
test_start_cluster_args = os.environ.get("TEST_START_CLUSTER_ARGS", "")
IMPALA_HOME = os.environ.get("IMPALA_HOME", "")

# Find the likely BuildType of the running Impala. Assume it's found through the path
# $IMPALA_HOME/be/build/latest as a fallback.
build_type_arg_regex = re.compile(r'--build_type=(\w+)', re.I)
build_type_arg_search_result = re.search(build_type_arg_regex, test_start_cluster_args)
if build_type_arg_search_result is not None:
  build_type_dir = build_type_search_result.groups()[0].lower()
else:
  build_type_dir = 'latest'

# Resolve any symlinks in the path.
impalad_basedir = \
    os.path.realpath(os.path.join(IMPALA_HOME, 'be/build', build_type_dir)).rstrip('/')

class SpecificImpaladBuildTypes:
  """
  Represents the possible CMAKE_BUILD_TYPE values. These specific build types are needed
  by Python test code, e.g. to set different timeouts for different builds. All values
  are lower-cased to enable case-insensitive comparison.
  """
  # ./buildall.sh -asan
  ADDRESS_SANITIZER = 'address_sanitizer'
  # ./buildall.sh
  DEBUG = 'debug'
  # ./buildall.sh -release
  RELEASE = 'release'
  # ./buildall.sh -codecoverage
  CODE_COVERAGE_DEBUG = 'code_coverage_debug'
  # ./buildall.sh -release -codecoverage
  CODE_COVERAGE_RELEASE = 'code_coverage_release'
  # ./buildall.sh -tsan
  TSAN = 'tsan'
  # ./buildall.sh -ubsan
  UBSAN = 'ubsan'

  VALID_BUILD_TYPES = [ADDRESS_SANITIZER, DEBUG, CODE_COVERAGE_DEBUG, RELEASE,
      CODE_COVERAGE_RELEASE, TSAN, UBSAN]

  @classmethod
  def detect(cls, impala_build_root):
    """
    Determine the build type based on the .cmake_build_type file created by
    ${IMPALA_HOME}/CMakeLists.txt. impala_build_root should be the path of the
    Impala source checkout, i.e. ${IMPALA_HOME}.
    """
    build_type_path = os.path.join(impala_build_root, ".cmake_build_type")
    try:
      with open(build_type_path) as build_type_file:
        build_type = build_type_file.read().strip().lower()
    except IOError:
      LOG.error("Could not open %s assuming DEBUG", build_type_path)
      return cls.DEBUG

    if build_type not in cls.VALID_BUILD_TYPES:
      raise Exception("Unknown build type {0}".format(build_type))
    LOG.debug("Build type detected: %s", build_type)
    return build_type



class ImpaladBuild(object):
  """
  Acquires and provides characteristics about the way the Impala under test was compiled
  and its likely effects on its responsiveness to automated test timings. Currently
  assumes that the Impala daemon under test was built in our current source checkout.
  TODO: we could get this information for remote cluster tests if we exposed the build
  type via a metric or the Impalad web UI.
  """
  def __init__(self, impala_build_root):
    self._specific_build_type = SpecificImpaladBuildTypes.detect(impala_build_root)

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
    return self.specific_build_type in (SpecificImpaladBuildTypes.CODE_COVERAGE_DEBUG,
                                        SpecificImpaladBuildTypes.CODE_COVERAGE_RELEASE)

  def is_asan(self):
    """
    Return whether the Impala under test was compiled with ASAN.
    """
    return self.specific_build_type == SpecificImpaladBuildTypes.ADDRESS_SANITIZER

  def is_tsan(self):
    """
    Return whether the Impala under test was compiled with TSAN.
    """
    return self.specific_build_type == SpecificImpaladBuildTypes.TSAN

  def is_ubsan(self):
    """
    Return whether the Impala under test was compiled with UBSAN.
    """
    return self.specific_build_type == SpecificImpaladBuildTypes.UBSAN

  def is_dev(self):
    """
    Return whether the Impala under test is a development build (i.e., any debug or ASAN
    build).
    """
    return self.specific_build_type in (
        SpecificImpaladBuildTypes.ADDRESS_SANITIZER, SpecificImpaladBuildTypes.DEBUG,
        SpecificImpaladBuildTypes.CODE_COVERAGE_DEBUG,
        SpecificImpaladBuildTypes.TSAN, SpecificImpaladBuildTypes.UBSAN)

  def runs_slowly(self):
    """
    Return whether the Impala under test "runs slowly". For our purposes this means
    either compiled with code coverage enabled or one of the sanitizers.
    """
    return self.has_code_coverage() or self.is_asan() or self.is_tsan() or self.is_ubsan()


IMPALAD_BUILD = ImpaladBuild(IMPALA_HOME)

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
