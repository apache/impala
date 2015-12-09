#!/bin/bash
# Copyright (c) 2015 Cloudera, Inc. All rights reserved.

set -euo pipefail
trap 'echo Error in $0 at line $LINENO: $(awk "NR == $LINENO" $0)' ERR

CLASSES=()
EXTRA_SHUTDOWN_TIME_SECS=1

while getopts :c:s: OPTION; do
  case $OPTION in
    c) CLASSES+=($OPTARG);;
    s) EXTRA_SHUTDOWN_TIME_SECS=$OPTARG;;
    *) echo "Usage: $0 -c <java class name> [-c ...] " \
          "[-s <wait time after stopping all processes>]"
      exit 1;;
  esac
done

if [[ ${#CLASSES[@]} -eq 0 ]]; then
  echo At least one class must be given >&2
  exit 1
fi

function pid_is_running {
  kill -0 $1 &>/dev/null
}

# Waits for 3 seconds for a pid to stop. Returns success if the pid is stopped otherwise
# returns failure.
function wait_for_pid_to_stop {
  for I in {1..30}; do
    if ! pid_is_running $1; then
      return
    fi
    sleep 0.1
  done
  return 1
}

NEEDS_EXTRA_WAIT=false
for CLASS in ${CLASSES[@]}; do
  PID=$(jps -m | (grep $CLASS || true) | awk '{print $1}')
  if [[ -z $PID ]]; then
    continue
  fi
  kill $PID || true   # Don't error if the process somehow died on its own.
  NEEDS_EXTRA_WAIT=true
  if wait_for_pid_to_stop $PID; then
    continue
  fi
  kill -9 $PID || true
  if ! wait_for_pid_to_stop $PID; then
    echo Unable to stop process $PID running java class $CLASS >&2
    exit 1
  fi
done
if $NEEDS_EXTRA_WAIT; then
  sleep $EXTRA_SHUTDOWN_TIME_SECS
fi
