#!/usr/bin/env bash
# Copyright (c) 2015 Cloudera, Inc. All rights reserved.

# Utility script that invokes maven but filters out noisy info logging
set -euo pipefail
echo "========================================================================"
echo "Running mvn $@"
echo "Directory: $(pwd)"
echo "========================================================================"
if ! mvn "$@" | grep -E -e WARNING -e ERROR -e SUCCESS -e FAILURE -e Test; then
  echo "mvn $@ exited with code $?"
  exit 1
fi
echo "------------------------------------------------------------------------"
echo
