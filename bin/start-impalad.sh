#!/bin/sh
. ${IMPALA_HOME}/bin/set-classpath.sh
$IMPALA_HOME/be/build/debug/service/impalad "$@"
