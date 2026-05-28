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

import pathlib
import unittest

import yaml


RBAC_PATH = pathlib.Path(__file__).resolve().parents[1] / "manifests" / "rbac.yaml"


class TestOperatorRbacManifest(unittest.TestCase):
    def test_manifest_does_not_bind_cluster_admin(self):
        content = RBAC_PATH.read_text(encoding="utf-8")
        self.assertNotIn("name: cluster-admin", content)
        self.assertIn("name: impala-operator-helm", content)
        self.assertIn("kind: ClusterRole", content)
        self.assertIn("kind: ClusterRoleBinding", content)

    def test_manifest_has_cr_status_and_no_namespace_create_permissions(self):
        content = RBAC_PATH.read_text(encoding="utf-8")
        docs = [doc for doc in yaml.safe_load_all(content) if doc]

        cluster_role_rules = []
        for doc in docs:
            if doc.get("kind") != "ClusterRole":
                continue
            cluster_role_rules.extend(doc.get("rules", []))

        self.assertTrue(cluster_role_rules)
        for rule in cluster_role_rules:
            self.assertNotIn("namespaces", rule.get("resources", []))

        status_resources = {"impalaclusters/status", "impalaclusters/finalizers"}
        status_rule = next(
            (
                rule
                for rule in cluster_role_rules
                if status_resources.issubset(set(rule.get("resources", [])))
            ),
            None,
        )
        self.assertIsNotNone(status_rule)
        self.assertEqual(set(status_rule["verbs"]), {"patch", "update"})

        crd_rule = next(
            (
                rule
                for rule in cluster_role_rules
                if "customresourcedefinitions" in rule.get("resources", [])
            ),
            None,
        )
        self.assertIsNotNone(crd_rule)
        self.assertIn("apiextensions.k8s.io", crd_rule.get("apiGroups", []))


if __name__ == "__main__":
    unittest.main()
