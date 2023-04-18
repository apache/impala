#!/usr/bin/env impala-python
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

# Assembles the artifacts required to build docker containers into a single directory.
# Most artifacts are symlinked so need to be dereferenced (e.g. with tar -h) before
# being used as a build context.

from __future__ import absolute_import, division, print_function
import argparse
import glob
import os
import shutil
from subprocess import check_call

parser = argparse.ArgumentParser()
parser.add_argument("--debug-build", help="Setup build context for debug build",
                    action="store_true")
parser.add_argument("--utility-context",
                    help="Setup utility build context instead of daemon",
                    action="store_true")
parser.add_argument("--output-dir", help="Directory to use for output")
args = parser.parse_args()

IMPALA_HOME = os.environ["IMPALA_HOME"]
BUILD_TYPE = "debug" if args.debug_build else "release"
OUTPUT_DIR = args.output_dir

IMPALA_TOOLCHAIN_PACKAGES_HOME = os.environ["IMPALA_TOOLCHAIN_PACKAGES_HOME"]
IMPALA_GCC_VERSION = os.environ["IMPALA_GCC_VERSION"]
IMPALA_BINUTILS_VERSION = os.environ["IMPALA_BINUTILS_VERSION"]
GCC_HOME = os.path.join(IMPALA_TOOLCHAIN_PACKAGES_HOME,
    "gcc-{0}".format(IMPALA_GCC_VERSION))
BINUTILS_HOME = os.path.join(
    IMPALA_TOOLCHAIN_PACKAGES_HOME, "binutils-{0}".format(IMPALA_BINUTILS_VERSION))
STRIP = os.path.join(BINUTILS_HOME, "bin/strip")
KUDU_HOME = os.environ["IMPALA_KUDU_HOME"]
KUDU_CLIENT_DIR = os.environ.get("KUDU_CLIENT_DIR")
# Different distributions put Kudu libraries in different places.
# Allow for libkudu_client.so to be in any of these locations:
#  - release/lib (used on Ubuntu)
#  - release/lib64 (used on Redhat)
#  - release/lib/exported
#
# Also, respect KUDU_CLIENT_DIR if it is set. With KUDU_CLIENT_DIR, the output
# is under KUDU_CLIENT_DIR/usr/local, but also varies on "lib" vs "lib64"
if KUDU_CLIENT_DIR:
  kudu_lib_dirs = [os.path.join(KUDU_CLIENT_DIR, "usr/local/lib"),
                   os.path.join(KUDU_CLIENT_DIR, "usr/local/lib64")]
else:
  kudu_lib_dirs = [os.path.join(KUDU_HOME, "release/lib"),
                   os.path.join(KUDU_HOME, "release/lib64"),
                   os.path.join(KUDU_HOME, "release/lib/exported")]

# Ensure the output directory exists and is empty.
if os.path.exists(OUTPUT_DIR):
    shutil.rmtree(OUTPUT_DIR)
os.makedirs(OUTPUT_DIR)

BIN_DIR = os.path.join(OUTPUT_DIR, "bin")

# Contains helper install scripts used during Docker build
HELPER_DIR = os.path.join(OUTPUT_DIR, "helper")

# Contains all library dependencies for Impala Executors.
EXEC_LIB_DIR = os.path.join(OUTPUT_DIR, "exec-lib")

# Contains all library dependencies for all Impala daemons.
LIB_DIR = os.path.join(OUTPUT_DIR, "lib")

# Contains all library dependencies for statestored.
# The statestore does not require any jar files since it does not run an embedded JVM.
STATESTORE_LIB_DIR = os.path.join(OUTPUT_DIR, "statestore-lib")

# We generate multiple library directories for the build context for daemons,
# but only a single one for the utility build context.
if args.utility_context:
  TARGET_LIB_DIRS = [LIB_DIR]
else:
  TARGET_LIB_DIRS = [LIB_DIR, EXEC_LIB_DIR, STATESTORE_LIB_DIR]

os.mkdir(BIN_DIR)
os.mkdir(HELPER_DIR)
for lib_dir in TARGET_LIB_DIRS:
  os.mkdir(lib_dir)


def symlink_file_into_dir(src_file, dst_dir):
  """Helper to symlink 'src_file' into 'dst_dir'."""
  os.symlink(src_file, os.path.join(dst_dir, os.path.basename(src_file)))


def symlink_file_into_dirs(src_file, dst_dirs):
  """Helper to symlink 'src_file' into all dirs in 'dst_dirs'."""
  for dst_dir in dst_dirs:
    symlink_file_into_dir(src_file, dst_dir)


def strip_debug_symbols(src_file, dst_dirs):
  """Strips debug symbols from the given 'src_file' and writes the output to the given
  'dst_dirs', with the same file name as the 'src_file'."""
  for dst_dir in dst_dirs:
    check_call([STRIP, "--strip-debug", src_file, "-o",
        os.path.join(dst_dir, os.path.basename(src_file))])

# Impala binaries and native dependencies.


# Strip debug symbols from release build to reduce image size. Keep them for
# debug build.
if args.utility_context:
  PROFILE_TOOL_BINARY = os.path.join(
      IMPALA_HOME, "be/build", BUILD_TYPE, "util/impala-profile-tool")
  if args.debug_build:
    symlink_file_into_dir(PROFILE_TOOL_BINARY, BIN_DIR)
  else:
    strip_debug_symbols(PROFILE_TOOL_BINARY, [BIN_DIR])
else:
  IMPALAD_BINARY = os.path.join(IMPALA_HOME, "be/build", BUILD_TYPE, "service/impalad")
  if args.debug_build:
    symlink_file_into_dir(IMPALAD_BINARY, BIN_DIR)
  else:
    strip_debug_symbols(IMPALAD_BINARY, [BIN_DIR])

# Add libstc++ binaries to LIB_DIR. Strip debug symbols for release builds.
found_libstdcpp_so = False
for libstdcpp_so in glob.glob(os.path.join(
    GCC_HOME, "lib64/{0}*.so*".format("libstdc++"))):
  # Ignore 'libstdc++.so.*-gdb.py'.
  if not os.path.basename(libstdcpp_so).endswith(".py"):
    found_libstdcpp_so = True
    dst_dirs = TARGET_LIB_DIRS
    if args.debug_build:
      symlink_file_into_dirs(libstdcpp_so, dst_dirs)
    else:
      strip_debug_symbols(libstdcpp_so, dst_dirs)

if not found_libstdcpp_so:
  raise Exception("No libstdc++.so found in search path: {0}".format(
      os.path.join(GCC_HOME, "lib64")))

# Add libgcc binaries to LIB_DIR.
found_libgcc_so = False
for libgcc_so in glob.glob(os.path.join(GCC_HOME, "lib64/{0}*.so*".format("libgcc_s"))):
  found_libgcc_so = True
  symlink_file_into_dirs(libgcc_so, TARGET_LIB_DIRS)

if not found_libgcc_so:
  raise Exception("No libgcc.so found in search path: {0}".format(
      os.path.join(GCC_HOME, "lib64")))

# Add libkudu_client binaries to LIB_DIR. Strip debug symbols for release builds.
found_kudu_so = False
for kudu_lib_dir in kudu_lib_dirs:
  for kudu_client_so in glob.glob(os.path.join(kudu_lib_dir, "libkudu_client.so*")):
    # All backend binaries currently link against libkudu_client.so even if they don't
    # need them.
    found_kudu_so = True
    dst_dirs = TARGET_LIB_DIRS
    if args.debug_build:
      symlink_file_into_dirs(kudu_client_so, dst_dirs)
    else:
      strip_debug_symbols(kudu_client_so, dst_dirs)

if not found_kudu_so:
  raise Exception("No Kudu shared object found in search path: {0}".format(kudu_lib_dirs))

# Add script for installing OS packages
symlink_file_into_dir(
    os.path.join(IMPALA_HOME, "docker/install_os_packages.sh"), HELPER_DIR)

if args.utility_context:
  symlink_file_into_dir(
      os.path.join(IMPALA_HOME, "docker/utility_entrypoint.sh"), BIN_DIR)
else:
  # Impala Coordinator dependencies.
  num_jars_on_classpath = 0
  dep_classpath = open(os.path.join(IMPALA_HOME, "fe/target/build-classpath.txt")).read()
  for jar in dep_classpath.split(":"):
    num_jars_on_classpath += 1
    assert os.path.exists(jar), "missing jar from classpath: {0}".format(jar)
    symlink_file_into_dir(jar, LIB_DIR)

  if num_jars_on_classpath == 0:
    raise Exception("No jars listed in {0}".format(os.path.join(IMPALA_HOME,
        "fe/target/build-classpath.txt")))

  # Impala Coordinator jars.
  num_frontend_jars = 0
  for jar in glob.glob(os.path.join(IMPALA_HOME, "fe/target/impala-frontend-*.jar")):
    # Ignore the tests jar
    if jar.find("-tests") != -1:
      continue
    symlink_file_into_dir(jar, LIB_DIR)
    num_frontend_jars += 1
  # There must be exactly one impala-frontend jar.
  assert num_frontend_jars == 1

  # Impala Executor dependencies.
  dep_classpath = open(os.path.join(IMPALA_HOME,
      "java/executor-deps/target/build-executor-deps-classpath.txt")).read()
  for jar in dep_classpath.split(":"):
    assert os.path.exists(jar), "missing jar from classpath: {0}".format(jar)
    symlink_file_into_dir(jar, EXEC_LIB_DIR)

  # Templates for debug web pages.
  os.symlink(os.path.join(IMPALA_HOME, "www"), os.path.join(OUTPUT_DIR, "www"))
  # Scripts
  symlink_file_into_dir(os.path.join(IMPALA_HOME, "docker/daemon_entrypoint.sh"), BIN_DIR)
  symlink_file_into_dir(os.path.join(IMPALA_HOME, "bin/graceful_shutdown_backends.sh"),
                        BIN_DIR)
