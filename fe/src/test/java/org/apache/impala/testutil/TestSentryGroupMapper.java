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

package org.apache.impala.testutil;

import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.hadoop.conf.Configuration;
import org.apache.sentry.core.common.exception.SentryGroupNotFoundException;
import org.apache.sentry.provider.common.GroupMappingService;

import java.util.Map;
import java.util.Set;

/**
 * This class is used for testing complex privileges where we don't create
 * users and groups on the system.
 */
public class TestSentryGroupMapper implements GroupMappingService {
  private final Map<String, Set<String>> groupsMap_ = Maps.newHashMap();

  public static final String SERVER_ADMIN = "server_admin";
  public static final String TEST_USER = "test_user";
  public static final String AUTH_TO_LOCAL = "auth_to_local_group";

  // Needed for sentry service to lookup groups.
  public TestSentryGroupMapper(Configuration conf, String resource) {
    this();
  }

  public TestSentryGroupMapper() {
    // Need to make sure we can resolve the dev user.
    String devUser = System.getProperty("user.name");
    groupsMap_.put(devUser, Sets.newHashSet(devUser));

    // User to groups for show_grant_user tests.
    groupsMap_.put("user_1group", Sets.newHashSet("group_1"));
    groupsMap_.put("user_2group", Sets.newHashSet("group_2a", "group_2b"));
    groupsMap_.put("user1_shared", Sets.newHashSet("group_3"));
    groupsMap_.put("user2_shared", Sets.newHashSet("group_3"));
    groupsMap_.put("user1_shared2", Sets.newHashSet("group_4a","group_4b"));
    groupsMap_.put("user2_shared2", Sets.newHashSet("group_4a"));

    // User to groups for test_owner_privilege tests.
    groupsMap_.put("oo_user1", Sets.newHashSet("oo_group1"));
    groupsMap_.put("oo_user2", Sets.newHashSet("oo_group2"));

    groupsMap_.put("foobar", Sets.newHashSet("foobar"));
    groupsMap_.put("FOOBAR", Sets.newHashSet("FOOBAR"));

    // User to groups for AuthorizationTest tests.
    groupsMap_.put("auth_to_local_user", Sets.newHashSet(AUTH_TO_LOCAL));
    groupsMap_.put("test_user", Sets.newHashSet(TEST_USER));
    groupsMap_.put("admin_user", Sets.newHashSet(SERVER_ADMIN));
  }

  @Override
  public Set<String> getGroups(String s) throws SentryGroupNotFoundException {
    Set<String> groups = groupsMap_.get(s);
    if (groups == null) {
      groups = Sets.newHashSet();
    }
    return groups;
  }
}
