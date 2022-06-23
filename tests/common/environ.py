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

import json
import logging
import os
import pytest
import re
import requests
import platform

LOG = logging.getLogger('tests.common.environ')
test_start_cluster_args = os.environ.get("TEST_START_CLUSTER_ARGS", "")
IMPALA_HOME = os.environ.get("IMPALA_HOME", "")
# TODO: IMPALA-8553: this is often inconsistent with the --testing_remote_cluster flag.
# Clarify the relationship and enforce that they are set correctly.
IMPALA_REMOTE_URL = os.environ.get("IMPALA_REMOTE_URL", "")

# Default web UI URL for local test cluster
DEFAULT_LOCAL_WEB_UI_URL = "http://localhost:25000"
DEFAULT_LOCAL_CATALOGD_WEB_UI_URL = "http://localhost:25020"

# Find the local build version. May be None if Impala wasn't built locally.
IMPALA_LOCAL_BUILD_VERSION = None
IMPALA_LOCAL_VERSION_INFO = os.path.join(IMPALA_HOME, "bin/version.info")
if os.path.isfile(IMPALA_LOCAL_VERSION_INFO):
  with open(IMPALA_LOCAL_VERSION_INFO) as f:
    for line in f:
      match = re.match("VERSION: ([^\s]*)\n", line)
      if match:
        IMPALA_LOCAL_BUILD_VERSION = match.group(1)
  if IMPALA_LOCAL_BUILD_VERSION is None:
    raise Exception("Could not find VERSION in {0}".format(IMPALA_LOCAL_VERSION_INFO))

# Check if it is Red Hat/CentOS/Rocky/AlmaLinux Linux
distribution = platform.linux_distribution()
distname = distribution[0].lower()
version = distribution[1]
IS_REDHAT_6_DERIVATIVE = False
IS_REDHAT_DERIVATIVE = False
if distname.find('centos') or distname.find('rocky') or \
   distname.find('almalinux') or distname.find('red hat'):
  IS_REDHAT_DERIVATIVE = True
  if len(re.findall('^6\.*', version)) > 0:
    IS_REDHAT_6_DERIVATIVE = True

# Find the likely BuildType of the running Impala. Assume it's found through the path
# $IMPALA_HOME/be/build/latest as a fallback.
build_type_arg_regex = re.compile(r'--build_type=(\w+)', re.I)
build_type_arg_search_result = re.search(build_type_arg_regex, test_start_cluster_args)
if build_type_arg_search_result is not None:
  build_type_dir = build_type_arg_search_result.groups()[0].lower()
else:
  build_type_dir = 'latest'

docker_network = None
docker_network_regex = re.compile(r'--docker_network=(\S+)', re.I)
docker_network_search_result = re.search(docker_network_regex, test_start_cluster_args)
if docker_network_search_result is not None:
  docker_network = docker_network_search_result.groups()[0]
IS_DOCKERIZED_TEST_CLUSTER = docker_network is not None

HIVE_MAJOR_VERSION = int(os.environ.get("IMPALA_HIVE_MAJOR_VERSION"))
if HIVE_MAJOR_VERSION > 2:
  MANAGED_WAREHOUSE_DIR = 'test-warehouse/managed'
else:
  MANAGED_WAREHOUSE_DIR = 'test-warehouse'
EXTERNAL_WAREHOUSE_DIR = 'test-warehouse'

# Resolve any symlinks in the path.
impalad_basedir = \
    os.path.realpath(os.path.join(IMPALA_HOME, 'be/build', build_type_dir)).rstrip('/')

# Detects if the platform is a version of Centos6 which may be affected by KUDU-1508.
# Default to the minimum kernel version which isn't affected by KUDU-1508 and parses
# the output of `uname -a` for the actual kernel version.
kernel_version = [2, 6, 32, 674]
kernel_release = os.uname()[2]
kernel_version_regex = re.compile(r'(\d+)\.(\d+)\.(\d+)\-(\d+).*')
kernel_version_match = kernel_version_regex.match(kernel_release)
if kernel_version_match is not None and len(kernel_version_match.groups()) == 4:
  kernel_version = map(lambda x: int(x), list(kernel_version_match.groups()))
IS_BUGGY_EL6_KERNEL = 'el6' in kernel_release and kernel_version < [2, 6, 32, 674]

class ImpalaBuildFlavors:
  """
  Represents the possible CMAKE_BUILD_TYPE values. These build flavors are needed
  by Python test code, e.g. to set different timeouts for different builds. All values
  are lower-cased to enable case-insensitive comparison.
  """
  # ./buildall.sh -asan
  ADDRESS_SANITIZER = 'address_sanitizer'
  # ./buildall.sh
  DEBUG = 'debug'
  # ./buildall.sh -debug_noopt
  DEBUG_NOOPT = 'debug_noopt'
  # ./buildall.sh -release
  RELEASE = 'release'
  # ./buildall.sh -codecoverage
  CODE_COVERAGE_DEBUG = 'code_coverage_debug'
  # ./buildall.sh -release -codecoverage
  CODE_COVERAGE_RELEASE = 'code_coverage_release'
  # ./buildall.sh -tidy
  TIDY = 'tidy'
  # ./buildall.sh -tsan
  TSAN = 'tsan'
  # ./buildall.sh -full_tsan
  TSAN_FULL = 'tsan_full'
  # ./buildall.sh -ubsan
  UBSAN = 'ubsan'
  # ./buildall.sh -full_ubsan
  UBSAN_FULL = 'ubsan_full'

  VALID_BUILD_TYPES = [ADDRESS_SANITIZER, DEBUG, DEBUG_NOOPT, CODE_COVERAGE_DEBUG,
       RELEASE, CODE_COVERAGE_RELEASE, TIDY, TSAN, TSAN_FULL, UBSAN, UBSAN_FULL]


class LinkTypes:
  """
  Represents the possible library link type values, either "dynamic" or "static". This
  value is derived from the cmake value of BUILD_SHARED_LIBS. All values are lower-cased
  to enable case-insensitive comparison.
  """
  # ./buildall.sh
  STATIC = 'static'
  # ./buildall.sh -build_shared_libs
  DYNAMIC = 'dynamic'

  VALID_LINK_TYPES = [STATIC, DYNAMIC]


class ImpalaTestClusterFlagsDetector:
  """
  Detects the build flags of different types of Impala clusters. Currently supports
  detecting build flags from either a locally built Impala cluster using a file generated
  by CMake, or from the Impala web ui, which is useful for detecting flags from a remote
  Impala cluster. The supported list of build flags is: [CMAKE_BUILD_TYPE,
  BUILD_SHARED_LIBS]
  """

  @classmethod
  def detect_using_build_root_or_web_ui(cls, impala_build_root):
    """
    Determine the build flags based on the .cmake_build_type file created by
    ${IMPALA_HOME}/CMakeLists.txt. impala_build_root should be the path of the
    Impala source checkout, i.e. ${IMPALA_HOME}. If .cmake_build_type is not present,
    or cannot be read, attempt to detect the build flags from the local web UI using
    detect_using_web_ui.
    """
    cmake_build_type_path = os.path.join(impala_build_root, ".cmake_build_type")
    try:
      with open(cmake_build_type_path) as cmake_build_type_file:
        build_flags = cmake_build_type_file.readlines()
        build_type = build_flags[0].strip().lower()
        build_shared_libs = build_flags[1].strip().lower()
    except IOError:
      LOG.debug("Unable to read .cmake_build_type file, fetching build flags from " +
              "web ui on localhost")
      build_type, build_shared_libs = ImpalaTestClusterFlagsDetector.detect_using_web_ui(
          DEFAULT_LOCAL_WEB_UI_URL)

    library_link_type = LinkTypes.STATIC if build_shared_libs == "off"\
                   else LinkTypes.DYNAMIC
    ImpalaTestClusterFlagsDetector.validate_build_flags(build_type, library_link_type)
    return build_type, library_link_type

  @classmethod
  def detect_using_web_ui(cls, impala_url):
    """
    Determine the build type based on the Impala cluster's web UI by using
    get_build_flags_from_web_ui.
    """
    build_flags = ImpalaTestClusterFlagsDetector.get_build_flags_from_web_ui(impala_url)
    build_type = build_flags['cmake_build_type']
    library_link_type = build_flags['library_link_type']
    ImpalaTestClusterFlagsDetector.validate_build_flags(build_type, library_link_type)
    return build_type, library_link_type

  @classmethod
  def validate_build_flags(cls, build_type, library_link_type):
    """
    Validates that the build flags have valid values.
    """
    if build_type not in ImpalaBuildFlavors.VALID_BUILD_TYPES:
      raise Exception("Unknown build type {0}".format(build_type))
    if library_link_type not in LinkTypes.VALID_LINK_TYPES:
      raise Exception("Unknown library link type {0}".format(library_link_type))
    LOG.debug("Build type detected: %s", build_type)
    LOG.debug("Library link type detected: %s", library_link_type)

  @classmethod
  def get_build_flags_from_web_ui(cls, impala_url):
    """
    Fetches the build flags from the given Impala cluster web UI by parsing the ?json
    response of the root homepage and looking for the section on build flags. It returns
    the flags as a dictionary where the key is the flag name.
    """
    response = requests.get(impala_url + "/?json")
    assert response.status_code == requests.codes.ok,\
            "Offending url: " + impala_url
    assert "application/json" in response.headers['Content-Type']

    build_flags_json = json.loads(response.text)["build_flags"]
    build_flags = dict((flag['flag_name'].lower(), flag['flag_value'].lower())
        for flag in build_flags_json)
    assert len(build_flags_json) == len(build_flags)  # Ensure there are no collisions
    return build_flags


class ImpalaTestClusterProperties(object):
  _instance = None

  """
  Acquires and provides characteristics about the way the Impala under test was compiled
  and its likely effects on its responsiveness to automated test timings.
  TODO: Support remote urls for catalogd web UI."""

  def __init__(self, build_flavor, library_link_type, web_ui_url,
      catalogd_web_ui_url=DEFAULT_LOCAL_CATALOGD_WEB_UI_URL):
    self._build_flavor = build_flavor
    self._library_link_type = library_link_type
    self._web_ui_url = web_ui_url
    self._catalogd_web_ui_url = catalogd_web_ui_url
    self._runtime_flags = None  # Lazily populated to avoid unnecessary web UI calls.
    self._catalogd_runtime_flags = None  # Lazily populated

  @classmethod
  def get_instance(cls):
    """Implements lazy initialization of a singleton instance of this class. We cannot
    initialize the instances when this module is imported because some dependencies may
    not be available yet, e.g. the pytest.config object. Thus we instead initialize it
    the first time that a test needs it."""
    if cls._instance is not None:
      return cls._instance

    web_ui_url = IMPALA_REMOTE_URL or DEFAULT_LOCAL_WEB_UI_URL
    if IMPALA_REMOTE_URL:
      # If IMPALA_REMOTE_URL is set, prefer detecting from the web UI.
      build_flavor, link_type =\
          ImpalaTestClusterFlagsDetector.detect_using_web_ui(web_ui_url)
    else:
      build_flavor, link_type =\
          ImpalaTestClusterFlagsDetector.detect_using_build_root_or_web_ui(IMPALA_HOME)
    cls._instance = ImpalaTestClusterProperties(build_flavor, link_type, web_ui_url)
    return cls._instance

  @property
  def build_flavor(self):
    """
    Return the correct ImpalaBuildFlavors for the Impala under test.
    """
    return self._build_flavor

  @property
  def library_link_type(self):
    """
    Return the library link type (either static or dynamic) for the Impala under test.
    """
    return self._library_link_type

  def has_code_coverage(self):
    """
    Return whether the Impala under test was compiled with code coverage enabled.
    """
    return self.build_flavor in (ImpalaBuildFlavors.CODE_COVERAGE_DEBUG,
                                 ImpalaBuildFlavors.CODE_COVERAGE_RELEASE)

  def is_asan(self):
    """
    Return whether the Impala under test was compiled with ASAN.
    """
    return self.build_flavor == ImpalaBuildFlavors.ADDRESS_SANITIZER

  def is_tsan(self):
    """
    Return whether the Impala under test was compiled with TSAN.
    """
    return self.build_flavor == ImpalaBuildFlavors.TSAN

  def is_ubsan(self):
    """
    Return whether the Impala under test was compiled with UBSAN.
    """
    return self.build_flavor == ImpalaBuildFlavors.UBSAN

  def is_dev(self):
    """
    Return whether the Impala under test is a development build (i.e., any debug or ASAN
    build).
    """
    return self.build_flavor in (
        ImpalaBuildFlavors.ADDRESS_SANITIZER, ImpalaBuildFlavors.DEBUG,
        ImpalaBuildFlavors.CODE_COVERAGE_DEBUG, ImpalaBuildFlavors.TSAN,
        ImpalaBuildFlavors.UBSAN)

  def runs_slowly(self):
    """
    Return whether the Impala under test "runs slowly". For our purposes this means
    either compiled with code coverage enabled or one of the sanitizers.
    """
    return self.has_code_coverage() or self.is_asan() or self.is_tsan() or self.is_ubsan()

  def is_statically_linked(self):
    """
    Return whether the Impala under test was statically linked during compilation.
    """
    return self.build_shared_libs == LinkTypes.STATIC

  def is_dynamically_linked(self):
    """
    Return whether the Impala under test was dynamically linked during compilation.
    """
    return self.build_shared_libs == LinkTypes.DYNAMIC

  def is_remote_cluster(self):
    """
    Return true if the Impala test cluster is running remotely, false otherwise.
    This should only be called from python tests once pytest has been initialised
    and pytest command line arguments are available.
    """
    assert hasattr(pytest, 'config'), "Must only be called from Python tests"
    # A remote cluster build can be indicated in multiple ways.
    return (IMPALA_REMOTE_URL or os.getenv("REMOTE_LOAD") or
            pytest.config.option.testing_remote_cluster)

  @property
  def runtime_flags(self):
    """Return the command line flags from the impala web UI. Returns a Python map with
    the flag name as the key and a dictionary of flag properties as the value."""
    if self._runtime_flags is None:
      response = requests.get(self._web_ui_url + "/varz?json")
      assert response.status_code == requests.codes.ok,\
              "Offending url: " + self._web_ui_url
      assert "application/json" in response.headers['Content-Type']
      self._runtime_flags = {}
      for flag_dict in json.loads(response.text)["flags"]:
        self._runtime_flags[flag_dict["name"]] = flag_dict
    return self._runtime_flags

  @property
  def catalogd_runtime_flags(self):
    """Return the command line flags from the catalogd web UI. Returns a Python map with
    the flag name as the key and a dictionary of flag properties as the value."""
    if self._catalogd_runtime_flags is None:
      response = requests.get(self._catalogd_web_ui_url + "/varz?json")
      assert response.status_code == requests.codes.ok,\
              "Offending url: " + self._catalogd_web_ui_url
      assert "application/json" in response.headers['Content-Type']
      self._catalogd_runtime_flags = {}
      for flag_dict in json.loads(response.text)["flags"]:
        self._catalogd_runtime_flags[flag_dict["name"]] = flag_dict
    return self._catalogd_runtime_flags

  def is_catalog_v2_cluster(self):
    """Checks whether we use local catalog."""
    try:
      key = "use_local_catalog"
      return key in self.runtime_flags and self.runtime_flags[key]["current"] == "true"
    except Exception:
      if self.is_remote_cluster():
        # IMPALA-8553: be more tolerant of failures on remote cluster builds.
        LOG.exception("Failed to get flags from web UI, assuming catalog V1")
        return False
      raise

  def is_event_polling_enabled(self):
    """Whether we use HMS notifications to automatically refresh catalog service.
    Checks if --hms_event_polling_interval_s is set to non-zero value"""
    try:
      key = "hms_event_polling_interval_s"
      return key in self.catalogd_runtime_flags and int(
        self._catalogd_runtime_flags[key]["current"]) > 0
    except Exception:
      if self.is_remote_cluster():
        # IMPALA-8553: be more tolerant of failures on remote cluster builds.
        LOG.exception(
          "Failed to get flags from web UI, assuming event polling is disabled")
        return False

def build_flavor_timeout(default_timeout, slow_build_timeout=None,
        asan_build_timeout=None, code_coverage_build_timeout=None):
  """
  Return a test environment-specific timeout based on the sort of ImpalaBuildFlavor under
  test.

  Required parameter: default_timeout - default timeout value. This applies when Impala is
  a standard release or debug build, or if no other timeouts are specified.

  Optional parameters:
  slow_build_timeout - timeout to use if we're running against *any* build known to be
  slow. If specified, this will preempt default_timeout if Impala is expected to be
  "slow". You can use this as a shorthand in lieu of specifying all of the following
  parameters.

  The parameters below correspond to build flavors. These preempt both
  slow_build_timeout and default_timeout, if the Impala under test is a build of the
  applicable type:

  asan_build_timeout - timeout to use if Impala with ASAN is running

  code_coverage_build_timeout - timeout to use if Impala with code coverage is running
  (both debug and release code coverage)
  """
  cluster_properties = ImpalaTestClusterProperties.get_instance()
  if cluster_properties.is_asan() and asan_build_timeout is not None:
    timeout_val = asan_build_timeout
  elif cluster_properties.has_code_coverage() and\
          code_coverage_build_timeout is not None:
    timeout_val = code_coverage_build_timeout
  elif cluster_properties.runs_slowly() and slow_build_timeout is not None:
    timeout_val = slow_build_timeout
  else:
    timeout_val = default_timeout
  return timeout_val
