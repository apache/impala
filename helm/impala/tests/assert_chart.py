#!/usr/bin/env impala-python3
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

import argparse
import sys
from pathlib import Path


def _read_text(path: str) -> str:
    return Path(path).read_text(encoding="utf-8")


def _require(
    failures: list[str],
    condition: bool,
    success_message: str,
    failure_message: str,
) -> None:
    if condition:
        print(f"[PASS] {success_message}")
    else:
        failures.append(failure_message)
        print(f"[FAIL] {failure_message}")


def _verify_default_images(default_render: str, failures: list[str]) -> None:
    expected_suffixes = [
        "catalogd",
        "impala_quickstart_hms",
        "impalad_coord_exec",
        "statestored",
    ]
    for suffix in expected_suffixes:
        image = f"image: apache/impala:4.5.0-{suffix}"
        _require(
            failures,
            image in default_render,
            f"default image renders {image}",
            f"default image missing {image}",
        )


def _verify_beeswax_absence(default_render: str, failures: list[str]) -> None:
    _require(
        failures,
        "beeswax" not in default_render.lower(),
        "beeswax name is absent from rendered manifests",
        "beeswax references found in rendered manifests",
    )
    _require(
        failures,
        "containerPort: 21000" not in default_render,
        "deprecated beeswax container port 21000 is absent",
        "deprecated beeswax container port 21000 was rendered",
    )
    _require(
        failures,
        "\n  - port: 21000\n" not in default_render,
        "deprecated beeswax service port 21000 is absent",
        "deprecated beeswax service port 21000 was rendered",
    )


def _verify_debug_levels(
    default_render: str,
    debug_render: str,
    failures: list[str],
) -> None:
    _require(
        failures,
        default_render.count("- -v=1") >= 3,
        "default debug levels render as -v=1 for daemons",
        "default debug levels did not render expected -v=1 args",
    )
    _require(
        failures,
        "- -v=3" in debug_render,
        "impalad.v override renders -v=3",
        "impalad.v override did not render -v=3",
    )
    _require(
        failures,
        "- -v=4" in debug_render,
        "catalogd.v override renders -v=4",
        "catalogd.v override did not render -v=4",
    )
    _require(
        failures,
        "- -v=5" in debug_render,
        "statestored.v override renders -v=5",
        "statestored.v override did not render -v=5",
    )


def _verify_hms_polling(
    default_render: str,
    polling_render: str,
    failures: list[str],
) -> None:
    _require(
        failures,
        "- -hms_event_polling_interval_s=1" in default_render,
        "default catalogd.hmsEventPollingIntervalS renders as 1",
        "default catalogd.hmsEventPollingIntervalS did not render as 1",
    )
    _require(
        failures,
        "- -hms_event_polling_interval_s=0" in polling_render,
        "catalogd.hmsEventPollingIntervalS override renders as 0",
        "catalogd.hmsEventPollingIntervalS override did not render as 0",
    )


def _verify_shared_warehouse_access_mode(
    default_render: str,
    failures: list[str],
) -> None:
    _require(
        failures,
        "- ReadWriteMany" in default_render,
        "default render uses ReadWriteMany for shared warehouse PVC",
        "default render did not use ReadWriteMany for shared warehouse PVC",
    )


def _verify_hms_security_context(hms_security_render: str, failures: list[str]) -> None:
    _require(
        failures,
        "runAsUser: 0" in hms_security_render,
        "hms.securityContext.runAsUser renders when set",
        "hms.securityContext.runAsUser did not render",
    )
    _require(
        failures,
        "runAsGroup: 0" in hms_security_render,
        "hms.securityContext.runAsGroup renders when set",
        "hms.securityContext.runAsGroup did not render",
    )


def _verify_ldap_render(ldap_render: str, failures: list[str]) -> None:
    _require(
        failures,
        "- -enable_ldap_auth" in ldap_render,
        "LDAP enable flag renders",
        "LDAP enable flag did not render",
    )
    _require(
        failures,
        "- -ldap_uri=ldap://ldap.example.org:389" in ldap_render,
        "LDAP URI flag renders",
        "LDAP URI flag did not render",
    )
    _require(
        failures,
        "- -ldap_bind_pattern=cn=#UID,dc=example,dc=org" in ldap_render,
        "LDAP bind pattern renders",
        "LDAP bind pattern did not render",
    )
    _require(
        failures,
        "- -ldap_passwords_in_clear_ok" in ldap_render,
        "LDAP cleartext-password opt-in flag renders",
        "LDAP cleartext-password opt-in flag did not render",
    )


def _verify_ranger_render(ranger_render: str, failures: list[str]) -> None:
    _require(
        failures,
        "# Source: impala/templates/ranger-deployment.yaml" in ranger_render,
        "Ranger deployment template renders when enabled",
        "Ranger deployment template did not render",
    )
    _require(
        failures,
        "# Source: impala/templates/ranger-service.yaml" in ranger_render,
        "Ranger service template renders when enabled",
        "Ranger service template did not render",
    )
    _require(
        failures,
        "- -authorization_provider=ranger" in ranger_render,
        "Impala daemons render Ranger authorization provider flag",
        "Ranger authorization provider flag did not render",
    )
    _require(
        failures,
        "- -server_name=server1" in ranger_render,
        "Ranger server_name flag renders with expected key format",
        "Ranger server_name flag did not render with expected key format",
    )
    _require(
        failures,
        "ranger-hive-security.xml" in ranger_render,
        "Ranger security configuration file is rendered and mounted",
        "Ranger security configuration file did not render",
    )
    _require(
        failures,
        "ranger-hive-audit.xml" in ranger_render,
        "Ranger audit configuration file is rendered and mounted",
        "Ranger audit configuration file did not render",
    )


def _verify_kudu_render(kudu_render: str, failures: list[str]) -> None:
    _require(
        failures,
        "# Source: impala/templates/kudu-master-deployment.yaml" in kudu_render,
        "Kudu master StatefulSet template renders when enabled",
        "Kudu master StatefulSet template did not render",
    )
    _require(
        failures,
        "# Source: impala/templates/kudu-tserver-deployment.yaml" in kudu_render,
        "Kudu tserver StatefulSet template renders when enabled",
        "Kudu tserver StatefulSet template did not render",
    )
    _require(
        failures,
        "# Source: impala/templates/kudu-master-service.yaml" in kudu_render,
        "Kudu master service template renders when enabled",
        "Kudu master service template did not render",
    )
    _require(
        failures,
        "# Source: impala/templates/kudu-tserver-service.yaml" in kudu_render,
        "Kudu tserver service template renders when enabled",
        "Kudu tserver service template did not render",
    )
    _require(
        failures,
        "- -kudu_master_hosts=impala-kudu-impala-kudu-master:7051" in kudu_render,
        "Impala daemons render Kudu master hosts when Kudu is enabled",
        "Kudu master hosts flag did not render in Impala daemons",
    )
    _require(
        failures,
        "name: kudu-master-data\n          emptyDir: {}" in kudu_render,
        "Kudu master data volume uses emptyDir when persistence is disabled",
        "Kudu master data volume did not render emptyDir for disabled persistence",
    )
    _require(
        failures,
        "name: kudu-tserver-data\n          emptyDir: {}" in kudu_render,
        "Kudu tserver data volume uses emptyDir when persistence is disabled",
        "Kudu tserver data volume did not render emptyDir for disabled persistence",
    )
    _require(
        failures,
        "volumeClaimTemplates:" not in kudu_render,
        "Kudu StatefulSets skip volumeClaimTemplates when persistence is disabled",
        "Kudu StatefulSets still render volumeClaimTemplates with disabled persistence",
    )
    _require(
        failures,
        "claimName:" not in kudu_render,
        "No PVC claimName mounts are rendered for disabled Kudu persistence",
        "PVC claimName mounts were rendered despite disabled Kudu persistence",
    )
    _require(
        failures,
        "# Source: impala/templates/kudu-pvc.yaml" not in kudu_render,
        "Kudu PVC template is skipped when Kudu persistence is disabled",
        "Kudu PVC template rendered despite disabled Kudu persistence",
    )


def _verify_oauth_render(oauth_render: str, failures: list[str]) -> None:
    _require(
        failures,
        "- -oauth_token_auth=true" in oauth_render,
        "OAuth token auth flag renders when enabled",
        "OAuth token auth flag did not render",
    )
    _require(
        failures,
        "- -oauth_jwt_custom_claim_username=sub" in oauth_render,
        "OAuth custom username claim flag renders",
        "OAuth custom username claim flag did not render",
    )
    _require(
        failures,
        "- -oauth_jwt_validate_signature=true" in oauth_render,
        "OAuth signature validation flag renders",
        "OAuth signature validation flag did not render",
    )
    _require(
        failures,
        "- -oauth_jwks_url=https://idp.example.org/jwks" in oauth_render,
        "OAuth JWKS URL flag renders",
        "OAuth JWKS URL flag did not render",
    )
    _require(
        failures,
        "- -oauth_allow_without_tls=true" in oauth_render,
        "OAuth no-TLS development override flag renders when set",
        "OAuth no-TLS development override flag did not render",
    )


def _verify_secure_render(secure_render: str, failures: list[str]) -> None:
    _require(
        failures,
        secure_render.count('sidecar.istio.io/inject: "true"') >= 4,
        "Istio sidecar annotation renders across core workloads",
        "Istio sidecar annotation did not render across core workloads",
    )
    _require(
        failures,
        "- -principal=impala/_HOST@EXAMPLE.COM" in secure_render,
        "Kerberos principal flag renders",
        "Kerberos principal flag did not render",
    )
    _require(
        failures,
        "- -be_principal=impala/_HOST@EXAMPLE.COM" in secure_render,
        "Kerberos backend principal flag renders",
        "Kerberos backend principal flag did not render",
    )
    _require(
        failures,
        "- -keytab_file=/etc/impala/security/impala.keytab" in secure_render,
        "Kerberos keytab path flag renders",
        "Kerberos keytab path flag did not render",
    )
    _require(
        failures,
        "- -krb5_conf=/etc/krb5/krb5.conf" in secure_render,
        "Kerberos krb5.conf path flag renders when config map is set",
        "Kerberos krb5.conf path flag did not render",
    )
    _require(
        failures,
        "- -ssl_server_certificate=/etc/impala/tls/tls.crt" in secure_render,
        "TLS certificate flag renders",
        "TLS certificate flag did not render",
    )
    _require(
        failures,
        "- -ssl_private_key=/etc/impala/tls/tls.key" in secure_render,
        "TLS private key flag renders",
        "TLS private key flag did not render",
    )
    _require(
        failures,
        "- -ssl_client_ca_certificate=/etc/impala/tls/ca.crt" in secure_render,
        "TLS client CA flag renders",
        "TLS client CA flag did not render",
    )
    _require(
        failures,
        "secretName: impala-kerberos-keytab" in secure_render,
        "Kerberos keytab secret mount renders",
        "Kerberos keytab secret mount did not render",
    )
    _require(
        failures,
        "secretName: impala-tls" in secure_render,
        "TLS secret mount renders",
        "TLS secret mount did not render",
    )
    _require(
        failures,
        "name: impala-krb5-conf" in secure_render,
        "krb5 config map mount renders when set",
        "krb5 config map mount did not render",
    )


def _verify_secure_validation_errors(
    missing_kerberos_principal_render: str,
    missing_kerberos_render: str,
    missing_tls_render: str,
    failures: list[str],
) -> None:
    _require(
        failures,
        "security.kerberos.principal is required" in missing_kerberos_principal_render,
        "Kerberos missing-principal validation error is raised",
        "Kerberos missing-principal validation error was not raised",
    )
    _require(
        failures,
        "security.kerberos.keytabSecretName is required" in missing_kerberos_render,
        "Kerberos missing-secret validation error is raised",
        "Kerberos missing-secret validation error was not raised",
    )
    _require(
        failures,
        "security.tls.secretName is required" in missing_tls_render,
        "TLS missing-secret validation error is raised",
        "TLS missing-secret validation error was not raised",
    )


def _verify_oauth_validation_errors(
    missing_oauth_jwks_render: str,
    ambiguous_oauth_jwks_render: str,
    failures: list[str],
) -> None:
    expected_error = (
        "Set exactly one of auth.oauth.jwksFilePath or auth.oauth.jwksUrl "
        "when auth.oauth.jwtValidateSignature=true"
    )
    _require(
        failures,
        expected_error in missing_oauth_jwks_render,
        "OAuth validation error is raised when JWKS source is missing",
        "OAuth validation error was not raised for missing JWKS source",
    )
    _require(
        failures,
        expected_error in ambiguous_oauth_jwks_render,
        "OAuth validation error is raised when both JWKS sources are set",
        "OAuth validation error was not raised for ambiguous JWKS source",
    )


def _verify_ranger_validation_error(
    missing_ranger_admin: str,
    failures: list[str],
) -> None:
    _require(
        failures,
        "auth.ranger.adminUrl is required when auth.ranger.enabled=true and "
        "ranger.enabled=false" in missing_ranger_admin,
        "Ranger external-mode validation error is raised without adminUrl",
        "Ranger external-mode validation error was not raised without adminUrl",
    )


def _verify_values_example_render(example_render: str, failures: list[str]) -> None:
    _require(
        failures,
        "- ReadWriteMany" in example_render,
        "values-example renders shared warehouse PVC with ReadWriteMany",
        "values-example did not render shared warehouse PVC with ReadWriteMany",
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Assert key behavior for Impala Helm chart renders."
    )
    parser.add_argument(
        "--default",
        required=True,
        help="Path to default render output",
    )
    parser.add_argument(
        "--debug",
        required=True,
        help="Path to debug-level override render",
    )
    parser.add_argument(
        "--polling",
        required=True,
        help="Path to hmsEventPollingIntervalS override render",
    )
    parser.add_argument(
        "--hms-security",
        required=True,
        help="Path to HMS securityContext override render",
    )
    parser.add_argument("--ldap", required=True, help="Path to LDAP render output")
    parser.add_argument("--ranger", required=True, help="Path to Ranger render output")
    parser.add_argument("--kudu", required=True, help="Path to Kudu render output")
    parser.add_argument("--oauth", required=True, help="Path to OAuth render output")
    parser.add_argument("--secure", required=True, help="Path to secure render output")
    parser.add_argument(
        "--missing-kerberos-principal",
        required=True,
        help="Path to secure render error output with missing kerberos principal",
    )
    parser.add_argument(
        "--missing-kerberos",
        required=True,
        help="Path to secure render error output with missing keytab secret",
    )
    parser.add_argument(
        "--missing-tls",
        required=True,
        help="Path to secure render error output with missing TLS secret",
    )
    parser.add_argument(
        "--missing-oauth-jwks",
        required=True,
        help="Path to OAuth render error output with no JWKS source configured",
    )
    parser.add_argument(
        "--ambiguous-oauth-jwks",
        required=True,
        help="Path to OAuth render error output with both JWKS sources configured",
    )
    parser.add_argument(
        "--missing-ranger-admin",
        required=True,
        help="Path to Ranger render error output with missing external admin URL",
    )
    parser.add_argument(
        "--example",
        required=True,
        help="Path to render output generated with values-example.yaml",
    )
    args = parser.parse_args()

    default_render = _read_text(args.default)
    debug_render = _read_text(args.debug)
    polling_render = _read_text(args.polling)
    hms_security_render = _read_text(args.hms_security)
    ldap_render = _read_text(args.ldap)
    ranger_render = _read_text(args.ranger)
    kudu_render = _read_text(args.kudu)
    oauth_render = _read_text(args.oauth)
    secure_render = _read_text(args.secure)
    missing_kerberos_principal_render = _read_text(args.missing_kerberos_principal)
    missing_kerberos_render = _read_text(args.missing_kerberos)
    missing_tls_render = _read_text(args.missing_tls)
    missing_oauth_jwks_render = _read_text(args.missing_oauth_jwks)
    ambiguous_oauth_jwks_render = _read_text(args.ambiguous_oauth_jwks)
    missing_ranger_admin = _read_text(args.missing_ranger_admin)
    example_render = _read_text(args.example)

    failures: list[str] = []

    _verify_default_images(default_render, failures)
    _verify_beeswax_absence(default_render, failures)
    _verify_debug_levels(default_render, debug_render, failures)
    _verify_hms_polling(default_render, polling_render, failures)
    _verify_shared_warehouse_access_mode(default_render, failures)
    _verify_hms_security_context(hms_security_render, failures)
    _verify_ldap_render(ldap_render, failures)
    _verify_ranger_render(ranger_render, failures)
    _verify_kudu_render(kudu_render, failures)
    _verify_oauth_render(oauth_render, failures)
    _verify_secure_render(secure_render, failures)
    _verify_secure_validation_errors(
        missing_kerberos_principal_render,
        missing_kerberos_render,
        missing_tls_render,
        failures,
    )
    _verify_oauth_validation_errors(
        missing_oauth_jwks_render,
        ambiguous_oauth_jwks_render,
        failures,
    )
    _verify_ranger_validation_error(missing_ranger_admin, failures)
    _verify_values_example_render(example_render, failures)

    if failures:
        print("\nChart assertions failed:")
        for failure in failures:
            print(f"- {failure}")
        return 1
    print("\nAll chart assertions passed.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
