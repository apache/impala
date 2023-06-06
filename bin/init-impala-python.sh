#!/usr/bin/env bash
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
# This is called during the build to initialize the impala-python
# virtualenv (which involves installing various packages and
# compiling things). This is not directly in CMake, because
# this depends on knowing IMPALA_HOME and other environment
# variables.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`
. "$bin"/impala-config.sh > /dev/null 2>&1

function print_usage {
  echo "init-impala-python.sh - Script called from CMake to init python venvs"
  echo "[-python3] : Init the python3 virtualenv (default is python2)"
}

IS_PYTHON3=false
while [ -n "$*" ]
do
  case "$1" in
    -python3)
       IS_PYTHON3=true
       ;;
    -help|*)
       print_usage
       exit 1
       ;;
  esac
  shift
done

cd $IMPALA_HOME
if $IS_PYTHON3 ; then
    bin/impala-python3 -c 'print("Initialized impala-python3")'
else
    bin/impala-python -c 'print("Initialized impala-python")'
fi
