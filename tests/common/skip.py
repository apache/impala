#!/usr/bin/env python
# Copyright (c) 2015 Cloudera, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Impala py.test skipif markers.  When a test can't be run against S3,
# choose the appropriate reason (or add a new one if needed) and
# annotate the class or test routine with the marker.
#

import os
import pytest

# TODO: move these to filesystem_utils.py
IS_DEFAULT_FS = not os.getenv("FILESYSTEM_PREFIX")
IS_S3 = os.getenv("TARGET_FILESYSTEM") == "s3"

# These ones are skipped due to product limitations.
skip_if_s3_insert = pytest.mark.skipif(IS_S3, reason="INSERT not implemented for S3")
skip_if_s3_load_data = pytest.mark.skipif(IS_S3, reason="LOAD DATA not implemented for S3")
skip_if_s3_caching = pytest.mark.skipif(IS_S3, reason="SET CACHED not implemented for S3")
skip_if_s3_hive = pytest.mark.skipif(IS_S3, reason="Hive doesn't work with S3")

# These ones need test infra work to re-enable.
skip_if_s3_udfs = pytest.mark.skipif(IS_S3, reason="udas/udfs not copied to S3")
skip_if_s3_datasrc = pytest.mark.skipif(IS_S3, reason="data sources not copied to S3")
skip_if_s3_hdfs_client = pytest.mark.skipif(IS_S3, reason="hdfs_client doesn't work with S3")
skip_if_s3_hbase = pytest.mark.skipif(IS_S3, reason="HBase not started with S3")
skip_if_s3_qualified_path = pytest.mark.skipif(IS_S3, reason="Tests rely on HDFS qualified paths, IMPALA-1872")

# Some tests require a non-default filesystem to be present.
skip_if_default_fs = pytest.mark.skipif(IS_DEFAULT_FS, reason="Non-default filesystem needed")
