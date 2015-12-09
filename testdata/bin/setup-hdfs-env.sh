#!/bin/bash
# Copyright (c) 2012 Cloudera, Inc. All rights reserved.
#

set -euo pipefail
trap 'echo Error in $0 at line $LINENO: $(awk "NR == $LINENO" $0)' ERR

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

if [[ $TARGET_FILESYSTEM == hdfs ]]; then  # Otherwise assume KMS isn't setup.
  # Create encryption keys for HDFS encryption tests. Keys are stored by the KMS.
  EXISTING_KEYS=$(hadoop key list)
  for KEY in testkey{1,2}; do
    if grep $KEY <<< $EXISTING_KEYS &>/dev/null; then
      hadoop key delete $KEY -f
    fi
    hadoop key create $KEY
  done
fi

# Create test cache pool
if hdfs cacheadmin -listPools testPool | grep testPool &>/dev/null; then
  hdfs cacheadmin -removePool testPool
fi
hdfs cacheadmin -addPool testPool ${CACHEADMIN_ARGS}

# Back to ourselves:
if [ "${PREVIOUS_PRINCIPAL}" != "" ]; then
  kinit -k -t ${KRB5_KTNAME} ${PREVIOUS_PRINCIPAL}
fi
