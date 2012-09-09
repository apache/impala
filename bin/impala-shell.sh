#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

SHELL_HOME=${IMPALA_SHELL_HOME:-${IMPALA_HOME}/shell}
export PYTHONPATH="$PYTHONPATH:${SHELL_HOME}/gen-py:${HIVE_HOME}/lib/py:${IMPALA_HOME}:/thirdparty/python-thrift-0.7.0" 
python ${SHELL_HOME}/impala_shell.py
