#!/usr/bin/env bash
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

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
CHART_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
if [[ -z "${IMPALA_HOME:-}" ]]; then
  IMPALA_HOME="$(cd "${SCRIPT_DIR}/../../.." && pwd)"
fi
export IMPALA_HOME
export PATH="${IMPALA_HOME}/bin:${PATH}"
TMP_DIR="$(mktemp -d)"

cleanup() {
  rm -rf "${TMP_DIR}"
}
trap cleanup EXIT

echo "Running helm lint..."
helm lint "${CHART_DIR}"

echo "Rendering default manifests..."
helm template impala-default "${CHART_DIR}" \
  > "${TMP_DIR}/default.yaml"

echo "Rendering debug level override manifests..."
helm template impala-debug "${CHART_DIR}" \
  --set impalad.v=3 \
  --set catalogd.v=4 \
  --set statestored.v=5 \
  > "${TMP_DIR}/debug.yaml"

echo "Rendering HMS polling compatibility manifests..."
helm template impala-polling "${CHART_DIR}" \
  --set catalogd.hmsEventPollingIntervalS=0 \
  > "${TMP_DIR}/polling.yaml"

echo "Rendering HMS securityContext manifests..."
helm template impala-hms-security "${CHART_DIR}" \
  --set hms.securityContext.runAsUser=0 \
  --set hms.securityContext.runAsGroup=0 \
  > "${TMP_DIR}/hms-security.yaml"

echo "Rendering LDAP auth manifests..."
helm template impala-ldap "${CHART_DIR}" \
  --set auth.ldap.enabled=true \
  --set auth.ldap.uri="ldap://ldap.example.org:389" \
  --set-string auth.ldap.bindPattern='cn=#UID\,dc=example\,dc=org' \
  --set auth.ldap.passwordsInClearOk=true \
  > "${TMP_DIR}/ldap.yaml"

echo "Rendering Ranger manifests..."
helm template impala-ranger "${CHART_DIR}" \
  --set ranger.enabled=true \
  --set auth.ranger.enabled=true \
  > "${TMP_DIR}/ranger.yaml"

echo "Rendering Kudu manifests with persistence disabled..."
helm template impala-kudu "${CHART_DIR}" \
  --set persistence.enabled=false \
  --set kudu.enabled=true \
  --set kudu.master.persistence.enabled=false \
  --set kudu.tserver.persistence.enabled=false \
  > "${TMP_DIR}/kudu.yaml"

echo "Rendering OAuth manifests..."
helm template impala-oauth "${CHART_DIR}" \
  --set auth.oauth.enabled=true \
  --set auth.oauth.jwksUrl="https://idp.example.org/jwks" \
  --set auth.oauth.jwtCustomClaimUsername="sub" \
  --set auth.oauth.allowWithoutTls=true \
  > "${TMP_DIR}/oauth.yaml"

echo "Rendering secure-cluster manifests..."
helm template impala-secure "${CHART_DIR}" \
  --set security.istio.enabled=true \
  --set security.kerberos.enabled=true \
  --set security.kerberos.principal="impala/_HOST@EXAMPLE.COM" \
  --set security.kerberos.bePrincipal="impala/_HOST@EXAMPLE.COM" \
  --set security.kerberos.keytabSecretName="impala-kerberos-keytab" \
  --set security.kerberos.krb5ConfigMapName="impala-krb5-conf" \
  --set security.tls.enabled=true \
  --set security.tls.secretName="impala-tls" \
  > "${TMP_DIR}/secure.yaml"

echo "Rendering values-example manifests..."
helm template impala-example "${CHART_DIR}" \
  -f "${CHART_DIR}/values-example.yaml" \
  > "${TMP_DIR}/example.yaml"

echo "Checking secure-cluster required-value validation..."
if helm template impala-secure-no-principal "${CHART_DIR}" \
  --set security.kerberos.enabled=true \
  --set security.kerberos.keytabSecretName="impala-kerberos-keytab" \
  > "${TMP_DIR}/missing-kerberos-principal.out" 2>&1; then
  echo "Expected helm template to fail without kerberos principal."
  exit 1
fi

if helm template impala-secure-no-keytab "${CHART_DIR}" \
  --set security.kerberos.enabled=true \
  --set security.kerberos.principal="impala/_HOST@EXAMPLE.COM" \
  > "${TMP_DIR}/missing-kerberos.out" 2>&1; then
  echo "Expected helm template to fail without kerberos keytab secret."
  exit 1
fi

if helm template impala-secure-no-tls-secret "${CHART_DIR}" \
  --set security.tls.enabled=true \
  > "${TMP_DIR}/missing-tls.out" 2>&1; then
  echo "Expected helm template to fail without TLS secret."
  exit 1
fi

if helm template impala-oauth-no-jwks "${CHART_DIR}" \
  --set auth.oauth.enabled=true \
  --set auth.oauth.jwtValidateSignature=true \
  > "${TMP_DIR}/missing-oauth-jwks.out" 2>&1; then
  echo "Expected helm template to fail without OAuth JWKS source."
  exit 1
fi

if helm template impala-oauth-ambiguous-jwks "${CHART_DIR}" \
  --set auth.oauth.enabled=true \
  --set auth.oauth.jwtValidateSignature=true \
  --set auth.oauth.jwksUrl="https://idp.example.org/jwks" \
  --set auth.oauth.jwksFilePath="/etc/impala/jwks.json" \
  > "${TMP_DIR}/ambiguous-oauth-jwks.out" 2>&1; then
  echo "Expected helm template to fail when both OAuth JWKS sources are set."
  exit 1
fi

if helm template impala-ranger-missing-admin "${CHART_DIR}" \
  --set auth.ranger.enabled=true \
  --set ranger.enabled=false \
  > "${TMP_DIR}/missing-ranger-admin.out" 2>&1; then
  echo "Expected helm template to fail without external Ranger admin URL."
  exit 1
fi

echo "Running chart assertions..."
"${SCRIPT_DIR}/assert_chart.py" \
  --default "${TMP_DIR}/default.yaml" \
  --debug "${TMP_DIR}/debug.yaml" \
  --polling "${TMP_DIR}/polling.yaml" \
  --hms-security "${TMP_DIR}/hms-security.yaml" \
  --ldap "${TMP_DIR}/ldap.yaml" \
  --ranger "${TMP_DIR}/ranger.yaml" \
  --kudu "${TMP_DIR}/kudu.yaml" \
  --oauth "${TMP_DIR}/oauth.yaml" \
  --secure "${TMP_DIR}/secure.yaml" \
  --missing-kerberos-principal "${TMP_DIR}/missing-kerberos-principal.out" \
  --missing-kerberos "${TMP_DIR}/missing-kerberos.out" \
  --missing-tls "${TMP_DIR}/missing-tls.out" \
  --missing-oauth-jwks "${TMP_DIR}/missing-oauth-jwks.out" \
  --ambiguous-oauth-jwks "${TMP_DIR}/ambiguous-oauth-jwks.out" \
  --missing-ranger-admin "${TMP_DIR}/missing-ranger-admin.out" \
  --example "${TMP_DIR}/example.yaml"

echo "Impala Helm chart tests completed successfully."
