# Copyright 2014 Cloudera Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# This file should be sourced by the minikdc and by impala-config.sh
# when operating a kerberized development environment.
#

# Twiddle this to turn on/off kerberos debug EVERYWHERE.  Then restart
# all deamons and the minikdc to enable lots of kerberos debug messages.
# Valid values are true | false
export MINIKDC_DEBUG=false

# MiniKdc realm configuration.  Unfortunately, it breaks if realm isn't
# EXAMPLE.COM.
export MINIKDC_ORG=EXAMPLE
export MINIKDC_DOMAIN=COM
export MINIKDC_REALM=${MINIKDC_ORG}.${MINIKDC_DOMAIN}

# Here are all the principals we'll ever need.  They are all here in
# variables in case we need to change anything.
#
# Notes on principals.
# * We don't use the YARN principal; instead we use impala so that all the
#   appropriate directories are owned by impala
# * We don't use the ${USER} principal, we use impala - for the same reasons.
# * Once I tried to swap out ${USER} for impala everywhere.  The impala daemons
#   came up fine, but some of the test code fell over.  Plus you have to tell
#   the impala shell that the principal isn't 'impala'.
#
export MINIKDC_PRINC_HDFS=hdfs/localhost@${MINIKDC_REALM}
export MINIKDC_PRINC_MAPR=mapred/localhost@${MINIKDC_REALM}
export MINIKDC_PRINC_YARN=yarn/localhost@${MINIKDC_REALM}
export MINIKDC_PRINC_HTTP=HTTP/localhost@${MINIKDC_REALM}
export MINIKDC_PRINC_HIVE=hive/localhost@${MINIKDC_REALM}
export MINIKDC_PRINC_HBSE=hbase/localhost@${MINIKDC_REALM}
export MINIKDC_PRINC_ZOOK=zookeeper/localhost@${MINIKDC_REALM}
export MINIKDC_PRINC_IMPALA=impala/localhost@${MINIKDC_REALM}
export MINIKDC_PRINC_IMPALA_BE=impala-be/localhost@${MINIKDC_REALM}
export MINIKDC_PRINC_USER=${USER}/localhost@${MINIKDC_REALM}
export MINIKDC_PRINC_LLAM=llama/localhost@${MINIKDC_REALM}

# Basic directory setup:
MINIKDC_SCRATCH_ROOT=${MINIKDC_SCRATCH_ROOT-${IMPALA_TEST_CLUSTER_LOG_DIR}}
export MINIKDC_WD=${MINIKDC_SCRATCH_ROOT}/minikdc-workdir

# The one big keytab created by the minikdc
export MINIKDC_KEYTAB=${MINIKDC_WD}/keytab

# The krb5.conf file everyone needs to point at
export MINIKDC_KRB5CONF=${MINIKDC_WD}/krb5.conf

# These options tell kerberos related code to emit lots of debug messages
if [ ${MINIKDC_DEBUG} = "true" ]; then
    export KRB5_TRACE="${MINIKDC_WD}/krb5.trace"
    export JAVA_KERBEROS_MAGIC="-Dsun.security.krb5.debug=true"
else
    unset KRB5_TRACE
    export JAVA_KERBEROS_MAGIC=""
fi

# Kerberos environment variables to talk to our MiniKdc
export KRB5_KTNAME="${MINIKDC_KEYTAB}"
export KRB5_CONFIG="${MINIKDC_KRB5CONF}"

# Shorthand for below
JSKC=java.security.krb5.conf

# Magic options so java can talk to this MiniKdc
export JAVA_KERBEROS_MAGIC="${JAVA_KERBEROS_MAGIC} -D${JSKC}=${MINIKDC_KRB5CONF}"

# Add the magic to HADOOP_OPTS so that hdfs can find the krb5.conf file
: ${HADOOP_OPTS=}
if [[ "${HADOOP_OPTS}" =~ "${JSKC}" ]]; then
  export HADOOP_OPTS="${HADOOP_OPTS} ${JAVA_KERBEROS_MAGIC}"
fi

# Ditto with YARN_OPTS
: ${YARN_OPTS=}
if [[ "${YARN_OPTS}" =~ "${JSKC}" ]]; then
  export YARN_OPTS="${YARN_OPTS} ${JAVA_KERBEROS_MAGIC}"
fi

unset JSKC

# And in case the above two don't work, this undocumented environment
# variable is be used to pass the krb5.conf into java.  This is required
# for MR jobs - everything else seems to work without it.
export _JAVA_OPTIONS="${JAVA_KERBEROS_MAGIC}"
