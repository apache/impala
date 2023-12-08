#!/bin/bash
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

# Do some error checking and generate junit symptoms after running a build.

set -euo pipefail
trap 'echo Error in $0 at line $LINENO: $(cd "'$PWD'" && awk "NR == $LINENO" $0)' ERR

if test -v CMAKE_BUILD_TYPE && [[ "${CMAKE_BUILD_TYPE}" =~ 'UBSAN' ]] \
    && [ "${UBSAN_FAIL}" = "error" ] \
    && { grep -rI ": runtime error: " "${IMPALA_HOME}/logs" 2>&1 | sort | uniq \
     | tee logs/ubsan.txt ; }
then
  "${IMPALA_HOME}"/bin/generate_junitxml.py --step UBSAN \
      --stderr "${IMPALA_HOME}"/logs/ubsan.txt --error "Undefined C++ behavior"
fi

rm -rf "${IMPALA_HOME}"/logs_system
mkdir -p "${IMPALA_HOME}"/logs_system
# Tolerate dmesg failures. dmesg can fail if there are insufficient permissions.
if dmesg > "${IMPALA_HOME}"/logs_system/dmesg 2>/dev/null ||
    sudo dmesg > "${IMPALA_HOME}"/logs_system/dmesg; then

  # Check dmesg for OOMs and generate a symptom if present.
  if [[ $(grep "Out of memory" "${IMPALA_HOME}"/logs_system/dmesg) ]]; then
    "${IMPALA_HOME}"/bin/generate_junitxml.py --phase finalize --step dmesg \
        --stdout "${IMPALA_HOME}"/logs_system/dmesg --error "Process was OOM killed."
  fi
else
  echo "Failed to run dmesg, not checking for OOMs"
fi

# Check for any minidumps and symbolize and dump them.
LOGS_DIR="${IMPALA_HOME}"/logs
if [[ $(find $LOGS_DIR -path "*minidumps*" -name "*dmp") ]]; then
  SYM_DIR=$(mktemp -d)
  dump_breakpad_symbols.py -b $IMPALA_HOME/be/build/latest -d $SYM_DIR
  for minidump in $(find $LOGS_DIR -path "*minidumps*" -name "*dmp"); do
    $IMPALA_TOOLCHAIN_PACKAGES_HOME/breakpad-$IMPALA_BREAKPAD_VERSION/bin/minidump_stackwalk \
        ${minidump} $SYM_DIR > ${minidump}_dumped 2> ${minidump}_dumped.log
    "${IMPALA_HOME}"/bin/generate_junitxml.py --phase finalize --step minidumps \
        --error "Minidump generated: $minidump" \
        --stderr "$(head -n 100 ${minidump}_dumped)"
  done
  rm -rf $SYM_DIR
fi

# Do a second pass over the minidumps with the resolve_minidump.py script.
# This means that we now generate two JUnitXMLs per minidump. This should
# be temporary.
# TODO: Once we verify everything works, we can remove the first loop.
if [[ $(find $LOGS_DIR -path "*minidumps*" -name "*dmp") ]]; then
  for minidump in $(find $LOGS_DIR -path "*minidumps*" -name "*dmp"); do
    # Since this is experimental, use it inside an if so that any error code doesn't
    # abort this script.
    if ! "${IMPALA_HOME}"/bin/resolve_minidumps.py --minidump_file ${minidump} \
        --output_file ${minidump}_dumpedv2 ; then
      echo "bin/resolve_minidumps.py failed!"
    else
      "${IMPALA_HOME}"/bin/generate_junitxml.py --phase finalize --step minidumpsv2 \
          --error "Minidump generated: $minidump" \
          --stderr "resolve_minidumps.py output:\n$(head -n 100 ${minidump}_dumpedv2)"
    fi
  done
fi

function check_for_asan_error {
  ERROR_LOG=${1}
  if grep -q "ERROR: AddressSanitizer:" ${ERROR_LOG} ; then
    # Extract out the ASAN message from the log file into a temp file.
    tmp_asan_output=$(mktemp)
    sed -n '/AddressSanitizer:/,/ABORTING/p' ${ERROR_LOG} > "${tmp_asan_output}"
    # Make each ASAN issue use its own JUnitXML file by including the log filename
    # in the step.
    base=$(basename ${ERROR_LOG})
    "${IMPALA_HOME}"/bin/generate_junitxml.py --phase finalize \
      --step "asan_error_${base}" \
      --error "Address Sanitizer message detected in ${ERROR_LOG}" \
      --stderr "$(cat ${tmp_asan_output})"
    rm "${tmp_asan_output}"
  fi
}

function check_for_tsan_error {
  ERROR_LOG=${1}
  if grep -q "WARNING: ThreadSanitizer:" ${ERROR_LOG} ; then
    # Extract out the TSAN message from the log file into a temp file.
    # Starts with WARNING: ThreadSanitizer and then ends with a line with several '='
    # characters (currently 18, we match 10).
    tmp_tsan_output=$(mktemp)
    sed -n '/ThreadSanitizer:/,/==========/p' ${ERROR_LOG} > "${tmp_tsan_output}"
    # Make each TSAN issue use its own JUnitXML file by including the log filename
    # in the step.
    base=$(basename ${ERROR_LOG})
    "${IMPALA_HOME}"/bin/generate_junitxml.py --phase finalize \
      --step "tsan_error_${base}" \
      --error "Thread Sanitizer message detected in ${ERROR_LOG}" \
      --stderr "$(cat ${tmp_tsan_output})"
    rm "${tmp_tsan_output}"
  fi
}

# Check for AddressSanitizer/ThreadSanitizer messages. ASAN/TSAN errors can show up
# in ERROR logs (particularly for impalad). Some backend tests generate ERROR logs.
for error_log in $(find $LOGS_DIR -name "*ERROR*"); do
  check_for_asan_error ${error_log}
  check_for_tsan_error ${error_log}
done
# Backend tests can also generate output in logs/be_tests/LastTest.log
if [[ -f ${LOGS_DIR}/be_tests/LastTest.log ]]; then
  check_for_asan_error ${LOGS_DIR}/be_tests/LastTest.log
  check_for_tsan_error ${LOGS_DIR}/be_tests/LastTest.log
fi

# Check for DCHECK messages. DCHECKs translate into CHECKs, which log at FATAL level
# and start the message with "Check failed:".
# Some backend tests do death tests that are designed to trigger DCHECKs. Ignore
# the be_tests directory to avoid flagging these as errors.
for fatal_log in $(find $LOGS_DIR -name "*FATAL*" ! -path "*/be_tests/*"); do
  if grep -q "Check failed:" "${fatal_log}"; then
    # Generate JUnitXML with the entire FATAL log included. It should be small.
    base=$(basename ${fatal_log})
    "${IMPALA_HOME}"/bin/generate_junitxml.py --phase finalize \
      --step "dcheck_${base}" \
      --error "DCHECK found in log file: ${fatal_log}" \
      --stderr "${fatal_log}"
  fi
done
