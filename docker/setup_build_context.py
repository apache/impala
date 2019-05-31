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

import glob
import os
import shutil
from subprocess import check_call

IMPALA_HOME = os.environ["IMPALA_HOME"]
OUTPUT_DIR = os.path.join(IMPALA_HOME, "docker/build_context")
DOCKERFILE = os.path.join(IMPALA_HOME, "docker/impala_base/Dockerfile")

IMPALA_TOOLCHAIN = os.environ["IMPALA_TOOLCHAIN"]
IMPALA_GCC_VERSION = os.environ["IMPALA_GCC_VERSION"]
IMPALA_BINUTILS_VERSION = os.environ["IMPALA_BINUTILS_VERSION"]
GCC_HOME = os.path.join(IMPALA_TOOLCHAIN, "gcc-{0}".format(IMPALA_GCC_VERSION))
BINUTILS_HOME = os.path.join(
    IMPALA_TOOLCHAIN, "binutils-{0}".format(IMPALA_BINUTILS_VERSION))
STRIP = os.path.join(BINUTILS_HOME, "bin/strip")
KUDU_HOME = os.environ["IMPALA_KUDU_HOME"]
KUDU_LIB_DIR = os.path.join(KUDU_HOME, "release/lib")

# Ensure the output directory exists and is empty.
if os.path.exists(OUTPUT_DIR):
    shutil.rmtree(OUTPUT_DIR)
os.makedirs(OUTPUT_DIR)

os.symlink(os.path.relpath(DOCKERFILE, OUTPUT_DIR),
    os.path.join(OUTPUT_DIR, "Dockerfile"))

BIN_DIR = os.path.join(OUTPUT_DIR, "bin")
LIB_DIR = os.path.join(OUTPUT_DIR, "lib")
os.mkdir(BIN_DIR)
os.mkdir(LIB_DIR)


def symlink_file_into_dir(src_file, dst_dir):
  """Helper to symlink 'src_file' into 'dst_dir'."""
  os.symlink(src_file, os.path.join(dst_dir, os.path.basename(src_file)))


# Impala binaries and native dependencies.
check_call([STRIP, "--strip-debug",
            os.path.join(IMPALA_HOME, "be/build/latest/service/impalad"),
            "-o", os.path.join(BIN_DIR, "impalad")])
for lib in ["libstdc++", "libgcc"]:
  for so in glob.glob(os.path.join(GCC_HOME, "lib64/{0}*.so*".format(lib))):
    symlink_file_into_dir(so, LIB_DIR)

for so in glob.glob(os.path.join(KUDU_LIB_DIR, "libkudu_client.so*")):
  symlink_file_into_dir(so, LIB_DIR)

# Impala dependencies.
dep_classpath = file(os.path.join(IMPALA_HOME, "fe/target/build-classpath.txt")).read()
for jar in dep_classpath.split(":"):
  assert os.path.exists(jar), "missing jar from classpath: {0}".format(jar)
  symlink_file_into_dir(jar, LIB_DIR)

# Impala jars.
for jar in glob.glob(os.path.join(IMPALA_HOME, "fe/target/impala-frontend-*.jar")):
  symlink_file_into_dir(jar, LIB_DIR)

# Templates for debug web pages.
os.symlink(os.path.join(IMPALA_HOME, "www"), os.path.join(OUTPUT_DIR, "www"))
# Scripts
symlink_file_into_dir(os.path.join(IMPALA_HOME, "docker/daemon_entrypoint.sh"), BIN_DIR)
