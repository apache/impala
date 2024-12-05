#!/usr/bin/env ambari-python-wrap
#
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
#
# Since dockerized-impala-bootstrap-and-test.sh does a full re-login
# as part of calling dockerized-impala-run-tests.sh, it loses
# environment variables. Preserving the environment variables are important
# for parameterized Jenkins jobs. This script takes a list of environment
# variables and preserves them by adding export statements to
# bin/impala-config-local.sh.
#
# Usage: dockerized-impala-preserve-vars.py [env var1] [env var2] ...
# If an environment variable is not defined in the current environment,
# it is omitted with a warning.

from __future__ import absolute_import, division, print_function
import sys
import os


def main():
  if len(sys.argv) <= 1:
    print("Usage: {0} [env vars]".format(sys.argv[0]))
    sys.exit(1)

  if "IMPALA_HOME" not in os.environ:
    print("ERROR: IMPALA_HOME must be defined")
    sys.exit(1)

  impala_home = os.environ["IMPALA_HOME"]
  # Append to the end of bin/impala-config-local.sh
  with open("{0}/bin/impala-config-local.sh".format(impala_home), "a") as f:
    for env_var in sys.argv[1:]:
      if env_var not in os.environ:
        print("{0} is not defined in the environment, skipping...".format(env_var))
        continue
      new_export = "export {0}=\"{1}\"".format(env_var, os.environ[env_var])
      print("Adding '{0}' to bin/impala-config-local.sh".format(new_export))
      f.write("{0}\n".format(new_export))


if __name__ == "__main__": main()
