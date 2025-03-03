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
#
# Tests for query expiration.

from __future__ import absolute_import, division, print_function
import pytest
import re

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite


class TestAIGenerateText(CustomClusterTestSuite):
  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @classmethod
  def setup_class(cls):
    if cls.exploration_strategy() != 'exhaustive':
      pytest.skip('runs only in exhaustive')
    super(TestAIGenerateText, cls).setup_class()

  # Using ai_generate_text_default
  ai_generate_text_default_query = """
      select ai_generate_text_default("test")
      """

  AI_GENERATE_COMMON_ERR_PREFIX = "AI Generate Text Error:"
  AI_CURL_NETWORK_ERR = "Network error: curl error"

  @pytest.mark.execute_serially
  def test_inaccessible_site(self):
    self._start_impala_cluster([
      '--impalad_args=--ai_additional_platforms="bad.site" '
      '--ai_endpoint="https://bad.site"'])
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    err = self.execute_query_expect_failure(client, self.ai_generate_text_default_query)
    assert re.search(re.escape(self.AI_GENERATE_COMMON_ERR_PREFIX), str(err))
    assert re.search(re.escape(self.AI_CURL_NETWORK_ERR), str(err))

  @pytest.mark.execute_serially
  def test_emptyjceks(self):
    self._start_impala_cluster([
      '--impalad_args=--ai_model="gpt-4" '
      '--ai_endpoint="https://api.openai.com/v1/chat/completions" '
      '--ai_api_key_jceks_secret=""'])
    impalad = self.cluster.get_any_impalad()
    client = impalad.service.create_beeswax_client()
    err = self.execute_query_expect_failure(client, self.ai_generate_text_default_query)
    assert re.search(re.escape(self.AI_GENERATE_COMMON_ERR_PREFIX), str(err))
    assert re.search(re.escape(self.AI_CURL_NETWORK_ERR), str(err))
