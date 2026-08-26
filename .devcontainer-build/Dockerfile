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

FROM ubuntu:22.04

ARG GIT_REPO
ARG GIT_BRANCH
ARG GIT_HASH
LABEL git.repo="${GIT_REPO}"
LABEL git.branch="${GIT_BRANCH}"
LABEL git.hash="${GIT_HASH}"

ARG USER_UID=1000
ARG USER_GID=${USER_UID}
ARG USER_NAME=impdev
ARG USER_HOME=/home/${USER_NAME}
ARG USER_SHELL=/usr/bin/zsh

ARG IMPALA_BUILD_THREADS=12
ARG IMPALA_CMAKE_VERSION
ARG IMPALA_GCC_VERSION
ARG IMPALA_TOOLCHAIN_PACKAGES_RELPATH
ARG DOCKER_BUILD=true
ARG JAVA_VERSION=17

LABEL java.version="${JAVA_VERSION}"

# General environment variables
ARG DEBIAN_FRONTEND=noninteractive
ENV IN_DOCKER=true

# Specifies the default timezone/locale.
ARG TZ="America/Los_Angeles"
ARG LOCALE=en_US.UTF-8

# Impala specific environment variables
ENV IMPALA_HOME=${USER_HOME}/impala
ENV IMPALA_CMAKE_VERSION=${IMPALA_CMAKE_VERSION}
ENV IMPALA_GCC_VERSION=${IMPALA_GCC_VERSION}
ENV IMPALA_TOOLCHAIN_PACKAGES_HOME=${IMPALA_HOME}/${IMPALA_TOOLCHAIN_PACKAGES_RELPATH}
ENV IMPALA_MINIMAL_DEBUG_INFO=true

# Debug UIs for Impala Daemon, Statestore, and Catalog
EXPOSE 25000-25009 25010 25020

# Impala HS2 and HS2-HTTP
EXPOSE 21050-21059 28000-28009

# Postgres for Hive Metastore
EXPOSE 5432

# Ranger UI
EXPOSE 6080

# HDFS NameNode
EXPOSE 5070

# Kudu Webserver
EXPOSE 8051

# Yarn ResourceManager
EXPOSE 8088

# Hive
EXPOSE 10001-10002

SHELL ["/bin/bash", "-c"]

RUN apt-get update \
    && apt-get install -y ca-certificates \
    && update-ca-certificates \
    && apt-get update \
    && apt-get install -y tzdata \
    && ln -snf "/usr/share/zoneinfo/${TZ}" /etc/localtime \
    && dpkg-reconfigure tzdata \
    && apt-get install -y sudo git binfmt-support psmisc vim emacs nano locales curl \
           wget openssh-server openssh-client dnsutils shellcheck bash-completion jq \
           uuid zsh ripgrep bubblewrap socat \
    && locale-gen en hu hi C \
    && update-locale "LANG=${LOCALE}" "LC_ALL=${LOCALE}" \
    && userdel -r ubuntu || true \
    && groupadd --gid "${USER_GID}" "${USER_NAME}" \
    && useradd --uid "${USER_UID}" --gid "${USER_GID}" --shell /bin/bash \
           --home "${USER_HOME}" --shell "${USER_SHELL}" "${USER_NAME}" \
    && passwd -l "${USER_NAME}" \
    && echo "${USER_NAME} ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers \
    && echo >> /etc/sysctl.conf \
    && echo "fs.inotify.max_user_watches=524288" >> /etc/sysctl.conf \
    && sysctl -p \
    && touch /.initial_config_not_done

USER ${USER_UID}:${USER_GID}

WORKDIR ${USER_HOME}/.ssh
RUN chmod 700 "${USER_HOME}/.ssh" \
    && echo "github.com ecdsa-sha2-nistp256 AAAAE2VjZHNhLXNoYTItbmlzdHAyNTYAAAAIbmlzdHAyNTYAAABBBEmKSENjQEezOmxkZMy7opKgwFB9nkt5YRrYMjNuG5N87uRgg6CLrbo5wAdT/y6v0mKV0U2w0WZ2YB/++Tpockg=" > known_hosts \
    && echo "github.com ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIOMqqnkVzrm0SdG6UOoqKLsabgH5C9okWi0dh2l9GKJl" >> known_hosts \
    && ssh-keyscan -p 29418 gerrit.cloudera.org >> known_hosts \
    && chmod 600 known_hosts

COPY .devcontainer-build/gdbinit ${USER_HOME}/.gdbinit

RUN --mount=type=bind,source=.,target=/tmp/impala \
    cp -R /tmp/impala/ "${IMPALA_HOME}"

WORKDIR ${IMPALA_HOME}
RUN echo "y" | sh -c "$(curl -fsSL https://raw.githubusercontent.com/ohmyzsh/ohmyzsh/master/tools/install.sh)" \
    && curl -Lo .git/hooks/commit-msg https://gerrit.cloudera.org/tools/hooks/commit-msg \
    && chmod 755 .git/hooks/commit-msg \
    && echo "[STATUS] - starting system bootstrap" \
    && echo "yes" | ./bin/bootstrap_system.sh \
    && echo "[STATUS] - sourcing impala-config.sh" \
    && . bin/impala-config.sh \
    && echo "[STATUS] - build all" \
    && ./buildall.sh -notests \
    && echo "[STATUS] - starting create-test-configuration.sh" \
    && ./bin/create-test-configuration.sh -create_metastore \
    && echo "[STATUS] - starting create_testdata.sh" \
    && ./bin/create_testdata.sh \
    && echo "[STATUS] - starting testdata load" \
    && sudo ssh-keygen -A \
    && sudo service ssh start \
    && sudo service postgresql start \
    && echo "[STATUS] - starting minicluster" \
    && . bin/impala-config.sh \
    && . bin/set-classpath.sh \
    && ./testdata/bin/run-all.sh \
    && echo "[STATUS] - starting Impala" \
    && ./bin/start-impala-cluster.py \
           --catalogd_args="-iceberg_allow_datafiles_in_table_location_only=false" \
    && echo "[STATUS] - loading testdata" \
    && ./bin/load-data.py --workloads=functional-query,tpch \
           --table_formats=text/none,parquet/none/none --exploration_strategy=exhaustive \
    && echo "[STATUS] - shutting down Impala and supporting services" \
    && ./bin/start-impala-cluster.py --kill_only \
    && ./testdata/bin/kill-all.sh \
    && sudo service postgresql stop \
    && sudo service ssh stop \
    && echo "[STATUS] - finished testdata load" \
    && echo "[STATUS] - deleting debug benchmark binaries" \
    && rm -rf be/build/debug/benchmarks \
    && echo "[STATUS] - finished deleting debug benchmark binaries" \
    && echo "[STATUS] - deleting log files" \
    && find "${IMPALA_LOGS_DIR}" -type f -delete \
    && echo "[STATUS] - finished deleting log files" \
    && echo "[STATUS] - cleaning/configuring local git repo" \
    && git reflog expire --expire=30.days.ago --all \
    && git gc --aggressive --prune=now \
    && for remote in $(git remote); do git remote rm "${remote}"; done \
    && git remote add upstream https://github.com/apache/impala.git \
    && rm -f "${USER_HOME}/.gitconfig" \
    && echo "[STATUS] - prepopulating Impala shell virtual environment" \
    && ./bin/impala-shell.sh --version \
    && echo "[STATUS] - finished prepopulating Impala shell virtual environment" \
    && echo "[STATUS] - configuring VSCode settings" \
    && cp -r .vscode-defaults .vscode \
    && esc_db_name=${METASTORE_DB//&/\\&}; sed -i '/"sqltools\.connections"/,$ s|"database":[[:space:]]*"[^"]*"|"database": "'"$esc_db_name"'"|' .vscode/settings.json \
    && ./.devcontainer-build/configure-jvm.sh \
    && echo "[STATUS] - finished configuring VSCode settings"

WORKDIR ${USER_HOME}/.ssh
RUN ls | grep -v -e authorized_keys -e config -e known_hosts \
        | xargs -I{} rm -rf {} || true \
    && > authorized_keys \
    && sudo touch /.regen-ssh-keys

COPY --chown=${USER_UID}:${USER_GID} .devcontainer-build/zshrc ${USER_HOME}/.zshrc

WORKDIR ${IMPALA_HOME}
