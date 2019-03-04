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

# This is a simple wrapper that runs the shell command specified by the arguments
# and generates a JUnitXML file if the command fails. It incorporates the output
# of the command into the JUnitXML file.
#
# This works best when it is invoking a single executable with arguments. There
# are some known limitations when invoked from the shell (as it would be if invoked
# by Make):
# 1. For a string of commands 'junitxml_command_wrapper.sh A && B && C', it only sees
#    the first one (A). The command A runs in its own shell, so any state it sets for
#    the shell is not seen in B && C. For example, if A = "cd directory", then B and
#    C would not see the changed directory.
# 2. For output piping 'junitxml_command_wrapper.sh A | B", again it only sees the
#    first one (A). It does leave the output unchanged (stdout remains stdout, stderr
#    remains stderr), but if command B fails, it will not generate JUnitXML.

COMMAND=("$@")

# Run the command, piping output to temporary files. Note that this output can
# contain Unicode characters, because g++ (and likely others) can generate smart
# quotes.
STDOUT_TMP_FILE=$(mktemp)
STDERR_TMP_FILE=$(mktemp)
# The command's stdout and stderr need to remain separate, and we tee them to separate
# files. Some CMake build steps have a command like "command1 | command2"
# and command2 should not see stderr. That also means that this script must not produce
# its own output when the command runs successfully.
"${COMMAND[@]}" > >(tee "${STDOUT_TMP_FILE}") 2> >(tee "${STDERR_TMP_FILE}" >&2)
COMMAND_RET_CODE=${PIPESTATUS[0]}
if [[ ${COMMAND_RET_CODE} -ne 0 ]]; then
  # Use a hash of the command to make sure multiple build failures generate distinct
  # symptoms
  # TODO: It would make sense to do some better parsing of the command to produce
  # a better filename.
  HASH=$(echo "${COMMAND[*]}" | md5sum | cut -d" " -f1)
  "${IMPALA_HOME}"/bin/generate_junitxml.py --phase build --step "${HASH}" \
    --error "Build command failed: ${COMMAND[*]}" \
    --stdout "$(head -n 1000 ${STDOUT_TMP_FILE})" \
    --stderr "$(head -n 1000 ${STDERR_TMP_FILE})"
fi
rm "${STDOUT_TMP_FILE}"
rm "${STDERR_TMP_FILE}"
exit "${COMMAND_RET_CODE}"
