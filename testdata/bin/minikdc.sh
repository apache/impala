#!/bin/bash
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

#
# Start, stop, and related actions upon a "MiniKdc".  This behaves as
# a normal KDC, but is self-contained and we control the principals.
#

# Exits with failure, printing its arguments
die() {
    echo "$0 ERROR: $@"
    exit 1
}

# Initial sanity checks
initial_checks() {
    if [ "${MINIKDC_HOME}" = "" ]; then
        die "MINIKDC_HOME environment variable not set"
    fi

    if [ ! -x "${MINIKDC_BIN}" ]; then
        die "${MINIKDC_BIN} absent or not executable"
    fi
}

# Create a one-shot directory to hold minikdc files.  Cleaned up in cleanup().
make_working_dir() {
    if [ ! -d ${MINIKDC_SCRATCH_ROOT} ]; then
        mkdir -p ${MINIKDC_SCRATCH_ROOT}
    fi
    MINIKDC_TMP=`mktemp -d --tmpdir=${MINIKDC_SCRATCH_ROOT} minikdc.XXXXXXXXXX`
    if [ ! -d ${MINIKDC_TMP} ]; then
        die "Failure creating working directory"
    fi
    ln -s `basename ${MINIKDC_TMP}` ${MINIKDC_WD}
}

# Remove directory above.  Called as a part of do_start() so the working
# directory will exist until the next instance of the minikdc is started.
cleanup() {
    rm -f ${MINIKDC_WD}
    rm -rf ${MINIKDC_SCRATCH_ROOT}/minikdc.*
}

# Echos the minikdc working directory
get_working_dir() {
    if [ -d "${MINIKDC_WD}" ]; then
        echo "${MINIKDC_WD}"
        return 0
    else
        return 1
    fi
}

# Is the minikdc running?
minikdc_running() {
    [ -d "${MINIKDC_WD}" ] || return 1
    pgrep -f llama-minikdc > /dev/null 2>&1
    return $?
}

# The MiniKdc reads from this properties file.
write_properties() {
    cat > ${MINIKDC_PROPS} <<EOF
org.name = ${MINIKDC_ORG}
org.domain = ${MINIKDC_DOMAIN}
kdc.port = ${MINIKDC_PORT}
debug = ${MINIKDC_DEBUG}
EOF
}

# Calls the "real" minikdc startup shell script from llama-minikdc; uses
# nohup to detach it from the terminal.
start_minikdc() {
    # Grabs all the principals and mutilates them into one desired string
    MINIKDC_PRINCIPALS=`env | grep MINIKDC_PRINC_ \
        | sed "s/@${MINIKDC_REALM}//g" \
        | awk -F= '{print $2}' \
        | tr '\n' ' '`

    if [ "${MINIKDC_DEBUG}" = "true" ]; then
        export MINIKDC_OPTS=-Dsun.security.krb5.debug=true
    fi

    nohup ${MINIKDC_BIN} ${MINIKDC_WD} ${MINIKDC_PROPS} ${MINIKDC_KEYTAB} \
        ${MINIKDC_PRINCIPALS} > ${MINIKDC_LOG} 2>&1 &
    sleep 1
    # Ought to be running instantaneously
    if ! minikdc_running; then
        die "MiniKdc failed to start"
    fi

    # But it takes a little while to become available
    TRIES=15
    STARTUPDONE=0
    while [ ${TRIES} -gt 0 ]; do
        if grep -q "^Standalone MiniKdc Running" ${MINIKDC_LOG}; then
            STARTUPDONE=1
            break
        fi
        printf .
        sleep 1
        TRIES=`expr ${TRIES} - 1`
    done

    if [ ${STARTUPDONE} -eq 0 ]; then
        do_stop
        die "MiniKdc failed to become available"
    fi
}

# Hunt down and destroy the minikdc.  Gently at first, then aggressively.
kill_minikdc() {
    TRIES=3
    DEAD=0
    while [ ${TRIES} -gt 0 ]; do
        if minikdc_running; then
            pkill -f llama-minikdc
            sleep 1
        else
            DEAD=1
            break;
        fi
        TRIES=`expr ${TRIES} - 1`
    done

    if [ ${DEAD} -eq 0 ]; then
        pkill -9 -f llama-minikdc
        sleep 1
        if minikdc_running; then
            die "Failed to kill the minikdc"
        fi
    fi
}

# Controlling function for 'start' command.
do_start() {
    if minikdc_running; then
        echo "The minikdc is already running."
        exit 0
    fi

    cleanup
    make_working_dir
    write_properties
    start_minikdc
    echo "Minikdc started successfully."
    return 0
}

# Controlling function for 'stop' command.
do_stop() {
    if ! minikdc_running; then
        echo "The minikdc is not running."
        return 0
    fi

    kill_minikdc
    echo "Minikdc stopped successfully."
}

#
# Execution starts here.
#

if [ ! -f "${MINIKDC_ENV}" ]; then
    die "Can't find MINIKDC_ENV: ${MINIKDC_ENV}"
fi
. ${MINIKDC_ENV}

# Interesting MiniKdc configuration:
MINIKDC_BIN=${MINIKDC_HOME}/bin/minikdc
MINIKDC_PROPS=${MINIKDC_WD}/properties.conf
MINIKDC_LOG=${MINIKDC_WD}/minikdc.log
MINIKDC_PORT=42574

initial_checks

case "$1" in
    start)
        do_start
        RV=$?
        ;;
    stop)
        do_stop
        RV=$?
        ;;
    restart)
        do_stop && do_start
        RV=$?
        ;;
    running|status)
        minikdc_running
        RV=$?
        ;;
    *)
        die "Usage: start|stop|restart|running|status"
        ;;
esac

exit ${RV}
