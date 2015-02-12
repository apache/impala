#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#

# Create a cache pool and encryption keys for tests

PREVIOUS_PRINCIPAL=""
CACHEADMIN_ARGS=""

# If we're kerberized, we need to become hdfs for this:
if ${CLUSTER_DIR}/admin is_kerberized; then
    PREVIOUS_PRINCIPAL=`klist | grep ^Default | awk '{print $3}'`
    PREVIOUS_USER=`echo ${PREVIOUS_PRINCIPAL} | awk -F/ '{print $1}'`
    CACHEADMIN_ARGS="-group supergroup -owner ${PREVIOUS_USER}"
    kinit -k -t ${KRB5_KTNAME} ${MINIKDC_PRINC_HDFS}
fi

hadoop key delete testkey1
hadoop key delete testkey2
hdfs cacheadmin -removePool testPool

# Create encryption keys for HDFS encryption tests. Keys are stored by the KMS.
hadoop key create testkey1
RV=$?
hadoop key create testkey2
if [ $RV -eq 0 ]; then RV=$?; fi

# Create test cache pool
hdfs cacheadmin -addPool testPool ${CACHEADMIN_ARGS}
if [ $RV -eq 0 ]; then RV=$?; fi

# Back to ourselves:
if [ "${PREVIOUS_PRINCIPAL}" != "" ]; then
    kinit -k -t ${KRB5_KTNAME} ${PREVIOUS_PRINCIPAL}
fi

exit ${RV}
