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

# Regenerates SSH keys for the current user and the host if the flag file
# /.regen-ssh-keys exists. This is necessary because SSH keys should not be
# shared between different devcontainer instances for security reasons.

set -euo pipefail

if [[ -f "/.regen-ssh-keys" ]]; then
  # Delete and regenerate usr ssh keys.
  CUR_USER="$(whoami)"
  NEW_KEY="${HOME}/.ssh/id_ed25519"
  echo "[INFO] Deleting all SSH keys for user '${CUR_USER}'."
  ls "${HOME}/.ssh" \
      | grep -v -e authorized_keys -e config -e known_hosts \
      | xargs -I{} rm -rf "${HOME}/.ssh/{}" || true
  echo "[INFO] Regenerating user '${CUR_USER}' SSH keys."
  ssh-keygen \
      -t ed25519 \
      -f "${NEW_KEY}" \
      -C "Impala-devcontainer-$(date +'%Y-%m-%d_%H:%M:%S') - Impala devcontainer '${HOSTNAME}' created '$(date)'" \
      -P ""
  cat "${NEW_KEY}.pub" >> "${HOME}/.ssh/authorized_keys"

  # Delete and regenerate host ssh keys.
  echo "[INFO] Regenerating host SSH keys."
  sudo rm -f /etc/ssh/ssh_host_*
  sudo ssh-keygen -A
  sudo service ssh restart

  sudo rm -f "/.regen-ssh-keys"
else
  echo "[INFO] SSH key regeneration not required."
fi
