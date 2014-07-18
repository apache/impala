#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#

PREVIOUS_PRINCIPAL=""
CACHEADMIN_ARGS=""

# If we're kerberized, we need to become hdfs for this:
if ${CLUSTER_DIR}/admin is_kerberized; then
    PREVIOUS_PRINCIPAL=`klist | grep ^Default | awk '{print $3}'`
    PREVIOUS_USER=`echo ${PREVIOUS_PRINCIPAL} | awk -F/ '{print $1}'`
    CACHEADMIN_ARGS="-group supergroup -owner ${PREVIOUS_USER}"
    kinit -k -t ${KRB5_KTNAME} ${MINIKDC_PRINC_HDFS}
fi

# Make a pool and cache some of the tables in memory.
hdfs cacheadmin -removePool testPool

hdfs cacheadmin -addPool testPool ${CACHEADMIN_ARGS}
RV=$?

# Back to ourselves:
if [ "${PREVIOUS_PRINCIPAL}" != "" ]; then
    kinit -k -t ${KRB5_KTNAME} ${PREVIOUS_PRINCIPAL}
fi

exit ${RV}
