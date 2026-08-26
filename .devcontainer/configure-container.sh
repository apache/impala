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

if [[ ! -f "/.initial_config_not_done" ]]; then
  exit 0
fi

echo
echo "Configuring git settings..."
echo

# Read in the file containing local host git settings if it exists
LOCAL_GIT_CFGS="/.impala-devcontainer-git"
if [[ -s "${LOCAL_GIT_CFGS}" ]]; then
  echo "Loading local git configurations from ${LOCAL_GIT_CFGS}"

  while IFS=' ' read -r key value; do
    if [[ -n "${key}" && -n "${value}" ]]; then
      git config --global "${key}" "${value}"
    fi
  done < "${LOCAL_GIT_CFGS}"
fi

# git config commands return 1 if the config is not found, thus do not exit on error
set +e

# Set git editor
if ! git config core.editor > /dev/null 2>&1; then
  echo "Select an editor for git:"
  echo "  1) emacs"
  echo "  2) nano"
  echo "  3) vim"
  read -rp "> " GIT_EDITOR
  echo
  case $GIT_EDITOR in
    1) git config --global core.editor "emacs" ;;
    2) git config --global core.editor "nano" ;;
    3) git config --global core.editor "vim" ;;
    *) echo "Invalid selection. No changes made." ;;
  esac
fi

# Set git username
if ! git config user.name > /dev/null 2>&1; then
  read -rp "Enter your git username> " GIT_USER
  echo
  if [[ -n "${GIT_USER}" ]]; then
    git config --global user.name "${GIT_USER}"
  fi
fi

# Set git email
if ! git config user.email > /dev/null 2>&1; then
  read -rp "Enter your git email> " GIT_EMAIL
  echo
  if [[ -n "${GIT_EMAIL}" ]]; then
    git config --global user.email "${GIT_EMAIL}"
  fi
fi

echo "Git editor is: $(git config --global core.editor)"
echo -n "Git user is:  "
git config --global user.name || echo "NOT SET"
echo -n "Git email is: "
git config --global user.email || echo "NOT SET"
echo

sudo rm -f /.initial_config_not_done
