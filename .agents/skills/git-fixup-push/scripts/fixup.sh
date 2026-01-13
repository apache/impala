#!/usr/bin/env bash
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

readonly SUBJECT_REGEX='IMPALA-\d{5,}'

dry_run=false
for arg in "$@"; do
  case "${arg}" in
    -h|--help)
      cat <<'EOF'
Usage: fixup.sh [--dry-run] [--help]

Combines commits from the most recent commit whose subject matches 'IMPALA-<id>' through
HEAD into a single commit.

Options:
  --dry-run  Print the selected commits and git commands without modifying history.
  -h, --help Show this help text and exit.
EOF
      exit 0
      ;;
    --dry-run)
      dry_run=true
      ;;
    *)
      echo "Unknown argument: ${arg}" >&2
      exit 1
      ;;
  esac
done

matched_line="$(git log --format='%H%x09%s' \
  | grep -m1 -P "\t.*${SUBJECT_REGEX}" || true)"

if [[ -z "${matched_line}" ]]; then
  echo "No commit found where the first line matches '${SUBJECT_REGEX}'." >&2
  exit 1
fi

commit_hash="${matched_line%%$'\t'*}"
commit_subject="${matched_line#*$'\t'}"

intermediate_commits_file="$(mktemp)"
trap 'rm -f "${intermediate_commits_file}"' EXIT

git log --format='%H %s' --reverse "${commit_hash}..HEAD" \
  > "${intermediate_commits_file}"

intermediate_commits=()
while IFS= read -r commit_line; do
  intermediate_commits+=("${commit_line}")
done < "${intermediate_commits_file}"

echo
echo "COMMIT LIST"
echo "-----------"
echo "${commit_hash:0:7} ${commit_subject}"

if [[ ${#intermediate_commits[@]} -gt 0 ]]; then
  for commit_line in "${intermediate_commits[@]}"; do
    echo "${commit_line:0:7} ${commit_line:41}"
  done
else
  echo
  echo "No commits to combine, exiting without modifying history."
  echo
  exit 0
fi

reset_count=$((${#intermediate_commits[@]} + 1))
reset_cmd=(git reset --soft "HEAD~${reset_count}")
commit_cmd=(git commit -C "${commit_hash}")

echo
echo "COMMANDS"
echo "--------"
echo "${reset_cmd[*]}"
echo "${commit_cmd[*]}"
echo

if [[ "${dry_run}" == true ]]; then
  exit 0
fi

"${reset_cmd[@]}"
"${commit_cmd[@]}"
