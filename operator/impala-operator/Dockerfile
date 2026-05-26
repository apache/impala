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

FROM python:3.14.6-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates \
    curl \
    tar \
    && rm -rf /var/lib/apt/lists/*

RUN HELM_VERSION="v3.16.2" && \
    curl -fsSL "https://get.helm.sh/helm-${HELM_VERSION}-linux-amd64.tar.gz" -o /tmp/helm.tgz && \
    tar -xzf /tmp/helm.tgz -C /tmp && \
    mv /tmp/linux-amd64/helm /usr/local/bin/helm && \
    chmod +x /usr/local/bin/helm && \
    rm -rf /tmp/helm.tgz /tmp/linux-amd64

WORKDIR /app

ENV DEFAULT_IMPALA_CHART_PATH=/charts/impala
ENV DEFAULT_IMPALA_VALUES_FILE=${DEFAULT_IMPALA_CHART_PATH}/values-example.yaml
ENV DEFAULT_LDAP_VALUES_FILE=${DEFAULT_IMPALA_CHART_PATH}/values-ldap-example.yaml
ENV DEFAULT_HELM_TIMEOUT_SECONDS=900

COPY operator/impala-operator/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY operator/impala-operator/main.py /app/main.py
COPY helm/impala ${DEFAULT_IMPALA_CHART_PATH}

CMD ["kopf", "run", "--verbose", "/app/main.py"]
