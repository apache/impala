#!/bin/sh
# Copyright 2012 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Starts up a single Impalad with the specified command line arguments. An optional
# -build_type parameter can be passed to determine the build type to use for the
# impalad instance.

set -e
# First run all the tests that need to be executed serially (namely insert tests)
py.test -v -x -m "execute_serially" --ignore="failure" "$@"

# Run the remaining tests in parallel
py.test -v -x -m "not execute_serially" --ignore="failure" -n 8 "$@"
