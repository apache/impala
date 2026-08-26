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

# Sets the container's timezone and locale based on files created by the devcontainer's
# initializeCommand.

if [[ -f "/.impala-devcontainer-tz" && -s "/.impala-devcontainer-tz" ]]; then
  TZ="$(cat /.impala-devcontainer-tz)"
  echo "[INFO] Setting container timezone to '${TZ}'."
  sudo ln -sf "/usr/share/zoneinfo/${TZ}" /etc/localtime
  sudo dpkg-reconfigure -fnoninteractive tzdata
else
  echo "[INFO] No timezone file found at '/.impala-devcontainer-tz'. Skipping timezone configuration."
fi

if [[ -f "/.impala-devcontainer-locale" && -s "/.impala-devcontainer-locale" ]]; then
  LOCALE="$(cat /.impala-devcontainer-locale)"
  echo "[INFO] Setting container locale to '${LOCALE}'."
  sudo update-locale LANG="${LOCALE}" LC_ALL="${LOCALE}"
else
  echo "[INFO] No locale file found at '/.impala-devcontainer-locale'. Skipping locale configuration."
fi
