#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.

set -euo pipefail
trap 'echo Error in $0 at line $LINENO: $(awk "NR == $LINENO" $0)' ERR

. ${IMPALA_HOME}/bin/set-pythonpath.sh

HIVE_SERVER_PORT=10000
export HIVE_SERVER2_THRIFT_PORT=11050
HIVE_METASTORE_PORT=9083
LOGDIR=${IMPALA_HOME}/cluster_logs/hive
HIVES2_TRANSPORT="plain_sasl"
METASTORE_TRANSPORT="buffered"
ONLY_METASTORE=0

CLUSTER_BIN=${IMPALA_HOME}/testdata/bin

if ${CLUSTER_DIR}/admin is_kerberized; then
    # Making a kerberized cluster... set some more environment variables.
    . ${MINIKDC_ENV}

    HIVES2_TRANSPORT="kerberos"
    # The metastore isn't kerberized yet:
    # METASTORE_TRANSPORT="kerberos"
fi

mkdir -p ${LOGDIR}

while [ -n "$*" ]
do
  case $1 in
    -only_metastore)
      ONLY_METASTORE=1
      ;;
    -help|-h|*)
      echo "run-hive-server.sh : Starts the hive server and the metastore."
      echo "[-metastore_only] : Only starts the hive metastore."
      exit 1;
      ;;
    esac
  shift;
done

# TODO: We should have a retry loop for every service we start.
# Kill for a clean start.
${CLUSTER_BIN}/kill-hive-server.sh &> /dev/null

# Starts a Hive Metastore Server on the specified port.
HADOOP_CLIENT_OPTS=-Xmx2024m hive --service metastore -p $HIVE_METASTORE_PORT \
    > ${LOGDIR}/hive-metastore.out 2>&1 &

# Wait for the Metastore to come up because HiveServer2 relies on it being live.
${CLUSTER_BIN}/wait-for-metastore.py --transport=${METASTORE_TRANSPORT}

if [ ${ONLY_METASTORE} -eq 0 ]; then
  # Starts a HiveServer2 instance on the port specified by the HIVE_SERVER2_THRIFT_PORT
  # environment variable.
  hive --service hiveserver2 > ${LOGDIR}/hive-server2.out 2>&1 &

  # Wait for the HiveServer2 service to come up because callers of this script
  # may rely on it being available.
  ${CLUSTER_BIN}/wait-for-hiveserver2.py --transport=${HIVES2_TRANSPORT}
fi
