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

set -euo pipefail

BINDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# To allow incrementally banning individual pylint checks, this uses grep
# expressions to match banned pylint warnings. The grep expressions are stored
# in the bin/banned_py3k_warnings.txt file.
BANNED_PY3K_WARNINGS="${BINDIR}/banned_py3k_warnings.txt"

function print_usage {
    echo "check-pylink-py3k.sh : Checks eligible python files for pylint py3k compliance."
    echo "Fails if the python files have py3k warnings that match the patterns in "
    echo "bin/banned_py3k_warnings.txt."
    echo "[--error_output_file] : (optional) Also output the errors to a file"
    echo "[--warning_output_file] : (optional) Also output the warnings to a file"
}

ERROR_OUTPUT_FILE=""
WARNING_OUTPUT_FILE=""
while [ -n "$*" ]
do
    case "$1" in
        --error_output_file)
            ERROR_OUTPUT_FILE="${2-}"
            shift;
            ;;
        --warning_output_file)
            WARNING_OUTPUT_FILE="${2-}"
            shift;
            ;;
        --help|*)
            print_usage
            exit 1
            ;;
    esac
    shift
done

pushd ${IMPALA_HOME} > /dev/null 2>&1

OUTPUT_TMP_DIR=$(mktemp -d)
PYLINT_OUTPUT_FILE="${OUTPUT_TMP_DIR}/pylint_output.txt"
ERROR_OUTPUT_TMP_FILE="${OUTPUT_TMP_DIR}/error_output_tmp.txt"
WARNING_OUTPUT_TMP_FILE="${OUTPUT_TMP_DIR}/warning_output_tmp.txt"

RETCODE=0
for file in $(git ls-files '**/*.py'); do
    # Skip the shell entirely (but cover tests/shell)
    if [[ "${file}" =~ "shell/" && ! "${file}" =~ "tests/shell" ]]; then
        continue
    fi
    # Ignore files that are created to run with python3.
    FIRST_LINE=$(head -n1 ${file})
    if [[ "${file}: ${FIRST_LINE}" =~ "#!" ]]; then
        if [[ "${FIRST_LINE}" =~ "python3" ]]; then
            >&2 echo "SKIPPING: ${file} is already using python3: ${FIRST_LINE}"
            continue
        fi
        if [[ "${FIRST_LINE}" =~ "/bin/bash" ]]; then
            >&2 echo "SKIPPING: ${file} is a weird bash/python hybrid: ${FIRST_LINE}"
            continue
        fi
    fi

    >&2 echo "PROCESSING: ${file}"

    # -s n (skip score for each file)
    # --exit-zero: don't fail
    impala-pylint -s n --exit-zero --py3k ${file} >> ${PYLINT_OUTPUT_FILE}
done

touch "${ERROR_OUTPUT_TMP_FILE}"
touch "${WARNING_OUTPUT_TMP_FILE}"

# Hitting a banned py3k warning will cause this to return an error
echo ""
echo ""
if grep -f "${BANNED_PY3K_WARNINGS}" "${PYLINT_OUTPUT_FILE}" > /dev/null 2>&1 ; then
    echo "ERROR: Some python files contain these banned pylint warnings:" | \
        tee "${ERROR_OUTPUT_TMP_FILE}"
    grep -f "${BANNED_PY3K_WARNINGS}" "${PYLINT_OUTPUT_FILE}" | \
        tee -a "${ERROR_OUTPUT_TMP_FILE}"
    RETCODE=1
else
    echo "No errors found" | tee "${ERROR_OUTPUT_TMP_FILE}"
fi

if [[ -n "${ERROR_OUTPUT_FILE}" ]]; then
    cp "${ERROR_OUTPUT_TMP_FILE}" "${ERROR_OUTPUT_FILE}"
fi

# The remaining py3k warnings are interesting, but they are not yet enforced.
# Pylint produces annoying lines like "************* Module X", so try to filter those out
echo ""
echo ""
if grep -v -e '\*\*\*\*' -f "${BANNED_PY3K_WARNINGS}" \
        "${PYLINT_OUTPUT_FILE}" > /dev/null 2>&1 ; then
    echo "WARNING: Some python files contain these unenforced pylint warnings:" | \
        tee "${WARNING_OUTPUT_TMP_FILE}"
    grep -v -e '\*\*\*\*' -f "${BANNED_PY3K_WARNINGS}" "${PYLINT_OUTPUT_FILE}" | \
        tee -a "${WARNING_OUTPUT_TMP_FILE}"

    echo "WARNING SUMMARY table:"
    cat "${WARNING_OUTPUT_TMP_FILE}" | grep -v "WARNING" | cut -d: -f4- | \
        sed 's#^ ##' | sort | uniq -c
else
    echo "No warnings found" | tee "${WARNING_OUTPUT_TMP_FILE}"
fi

if [[ -n "${WARNING_OUTPUT_FILE}" ]]; then
    cp "${WARNING_OUTPUT_TMP_FILE}" "${WARNING_OUTPUT_FILE}"
fi

rm -rf "${OUTPUT_TMP_DIR}"

popd > /dev/null 2>&1

exit ${RETCODE}
