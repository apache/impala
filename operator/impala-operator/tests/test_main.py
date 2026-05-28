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

import importlib.util
import pathlib
import sys
import types
import unittest
from unittest import mock


def _identity_decorator(*_args, **_kwargs):
    def _wrapper(func):
        return func

    return _wrapper


def _load_main_module():
    module_name = "impala_operator_main_under_test"
    if module_name in sys.modules:
        return sys.modules[module_name]

    module_path = pathlib.Path(__file__).resolve().parents[1] / "main.py"

    fake_kopf = types.ModuleType("kopf")
    fake_kopf.on = types.SimpleNamespace(
        startup=_identity_decorator,
        create=_identity_decorator,
        update=_identity_decorator,
        delete=_identity_decorator,
    )
    fake_kopf.OperatorSettings = object

    fake_kubernetes = types.ModuleType("kubernetes")
    fake_client = types.ModuleType("kubernetes.client")
    fake_exceptions = types.ModuleType("kubernetes.client.exceptions")
    fake_config = types.ModuleType("kubernetes.config")

    class FakeApiException(Exception):
        def __init__(self, status=None):
            super().__init__(status)
            self.status = status

    fake_exceptions.ApiException = FakeApiException
    fake_client.exceptions = fake_exceptions
    fake_client.CoreV1Api = object
    fake_client.V1Namespace = object
    fake_client.V1ObjectMeta = object

    fake_config.ConfigException = type("FakeConfigException", (Exception,), {})
    fake_config.load_incluster_config = lambda: None
    fake_config.load_kube_config = lambda: None

    fake_kubernetes.client = fake_client
    fake_kubernetes.config = fake_config

    with mock.patch.dict(
        sys.modules,
        {
            "kopf": fake_kopf,
            "kubernetes": fake_kubernetes,
            "kubernetes.client": fake_client,
            "kubernetes.client.exceptions": fake_exceptions,
            "kubernetes.config": fake_config,
        },
    ):
        spec = importlib.util.spec_from_file_location(module_name, module_path)
        module = importlib.util.module_from_spec(spec)
        assert spec and spec.loader
        spec.loader.exec_module(module)
        sys.modules[module_name] = module
        return module


MAIN = _load_main_module()


def _set_args_to_dict(args):
    result = {}
    prefixes = {}
    for idx in range(0, len(args), 2):
        if args[idx] not in ("--set", "--set-string"):
            raise AssertionError("Unexpected arg prefix: %s" % args[idx])
        key, value = args[idx + 1].split("=", 1)
        result[key] = value
        prefixes[key] = args[idx]
    return result, prefixes


class TestOperatorSetArgs(unittest.TestCase):
    def test_set_args_renders_sorted_flags_and_query_defaults(self):
        args = MAIN._set_args(
            {
                "config": {
                    "impalad": {
                        "flags": {"zflag": "2", "aflag": "1"},
                        "queryDefaults": {
                            "mt_dop": "4",
                            "default_file_format": "parquet",
                        },
                    },
                    "catalogd": {"flags": {"catalog": "true"}},
                    "statestored": {"flags": {"enable_feature": "1"}},
                    "hms": {"flags": {"hms_threads": "8"}},
                }
            }
        )
        values, _ = _set_args_to_dict(args)

        self.assertEqual(values["impalad.extraArgs[0]"], "-aflag=1")
        self.assertEqual(values["impalad.extraArgs[1]"], "-zflag=2")
        self.assertEqual(
            values["impalad.defaultQueryOptions"],
            "default_file_format=parquet\\,mt_dop=4",
        )
        self.assertEqual(values["catalogd.extraArgs[0]"], "-catalog=true")
        self.assertEqual(values["statestored.extraArgs[0]"], "-enable_feature=1")
        self.assertEqual(values["hms.extraArgs[0]"], "-hms_threads=8")

    def test_set_args_escapes_ldap_bind_pattern_and_set_overrides(self):
        args = MAIN._set_args(
            {
                "kuduEnabled": True,
                "ldapEnabled": True,
                "ldapBindPattern": "cn=#UID,dc=example,dc=org",
                "set": {
                    "kudu.enabled": "false",
                    "custom.flag": "value,with,commas",
                },
            }
        )
        values, prefixes = _set_args_to_dict(args)

        self.assertEqual(values["auth.ldap.bindPattern"], "cn=#UID\\,dc=example\\,dc=org")
        self.assertEqual(prefixes["auth.ldap.bindPattern"], "--set-string")
        self.assertEqual(values["kudu.enabled"], "false")
        self.assertEqual(values["custom.flag"], "value\\,with\\,commas")

    def test_sanitize_cmd_redacts_sensitive_set_values(self):
        cmd = [
            "helm",
            "upgrade",
            "--set",
            "auth.ldap.bindPassword=supersecret",
            "--set-string",
            "oauth.clientSecret=abcd",
            "--set",
            "service.type=ClusterIP",
        ]
        sanitized = MAIN._sanitize_cmd(cmd)
        self.assertIn("auth.ldap.bindPassword=<redacted>", sanitized)
        self.assertIn("oauth.clientSecret=<redacted>", sanitized)
        self.assertIn("service.type=ClusterIP", sanitized)
        self.assertNotIn("supersecret", sanitized)
        self.assertNotIn("=abcd", sanitized)

    def test_reconcile_ignores_spec_namespace_override(self):
        logger = mock.Mock()
        patch = mock.Mock()
        patch.status = {}
        with mock.patch.object(MAIN, "_ensure_ldap") as ensure_ldap, mock.patch.object(
            MAIN, "_ensure_impala"
        ) as ensure_impala:
            MAIN.reconcile(
                spec={"namespace": "other"},
                name="impala-demo",
                namespace="impala",
                patch=patch,
                logger=logger,
                meta={"generation": 3},
            )

        ensure_ldap.assert_called_once_with({"namespace": "other"}, "impala", logger)
        ensure_impala.assert_called_once_with({"namespace": "other"}, "impala", logger)
        logger.warning.assert_called_once()
        self.assertEqual(patch.status["targetNamespace"], "impala")

    def test_ensure_ldap_uninstalls_existing_release_when_disabled(self):
        logger = mock.Mock()
        with mock.patch.object(
            MAIN, "_release_exists", return_value=True
        ), mock.patch.object(
            MAIN, "_delete_release"
        ) as delete_release, mock.patch.object(
            MAIN, "_run"
        ) as run_cmd:
            MAIN._ensure_ldap(
                {
                    "ldapEnabled": False,
                    "ldapReleaseName": "impala-ldap",
                },
                "impala",
                logger,
            )

        delete_release.assert_called_once_with("impala-ldap", "impala", logger)
        run_cmd.assert_not_called()

    def test_ensure_ldap_skips_uninstall_when_release_absent(self):
        logger = mock.Mock()
        with mock.patch.object(
            MAIN, "_release_exists", return_value=False
        ), mock.patch.object(MAIN, "_delete_release") as delete_release:
            MAIN._ensure_ldap(
                {
                    "ldapEnabled": False,
                    "ldapReleaseName": "impala-ldap",
                },
                "impala",
                logger,
            )

        delete_release.assert_not_called()


if __name__ == "__main__":
    unittest.main()
