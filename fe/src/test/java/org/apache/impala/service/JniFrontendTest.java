// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package org.apache.impala.service;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.security.JniBasedUnixGroupsMappingWithFallback;
import org.apache.hadoop.security.JniBasedUnixGroupsNetgroupMappingWithFallback;
import org.apache.hadoop.security.ShellBasedUnixGroupsMapping;
import org.apache.hadoop.security.ShellBasedUnixGroupsNetgroupMapping;
import org.apache.impala.common.ImpalaException;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JniFrontendTest {
  @Test
  public void testCheckGroupsMappingProvider() throws ImpalaException {
    Configuration conf = new Configuration();
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        JniBasedUnixGroupsMappingWithFallback.class.getName());
    assertTrue(JniFrontend.checkGroupsMappingProvider(conf).isEmpty());

    conf = new Configuration();
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        ShellBasedUnixGroupsMapping.class.getName());
    assertEquals(JniFrontend.checkGroupsMappingProvider(conf),
        String.format("Hadoop groups mapping provider: %s is known to be problematic. " +
            "Consider using: %s instead.",
            ShellBasedUnixGroupsMapping.class.getName(),
            JniBasedUnixGroupsMappingWithFallback.class.getName()));

    conf = new Configuration();
    conf.set(CommonConfigurationKeys.HADOOP_SECURITY_GROUP_MAPPING,
        ShellBasedUnixGroupsNetgroupMapping.class.getName());
    assertEquals(JniFrontend.checkGroupsMappingProvider(conf),
        String.format("Hadoop groups mapping provider: %s is known to be problematic. " +
            "Consider using: %s instead.",
            ShellBasedUnixGroupsNetgroupMapping.class.getName(),
            JniBasedUnixGroupsNetgroupMappingWithFallback.class.getName()));
  }
}