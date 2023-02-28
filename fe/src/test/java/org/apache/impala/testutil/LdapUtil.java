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

public class LdapUtil {
  // These correspond to the values in fe/src/test/resources/users.ldif
  public static final String TEST_USER_1 = "Test1Ldap";
  public static final String TEST_USER_DN_1 = "cn=Test1Ldap,ou=Users,dc=myorg,dc=com";
  public static final String TEST_PASSWORD_1 = "12345";

  public static final String TEST_USER_2 = "Test2Ldap";
  public static final String TEST_PASSWORD_2 = "abcde";

  public static final String TEST_USER_3 = "Test3Ldap";
  public static final String TEST_PASSWORD_3 = "67890";

  public static final String TEST_USER_4 = "Test4Ldap";
  public static final String TEST_PASSWORD_4 = "fghij";

  public static final String TEST_USER_5 = "Test5Ldap";
  public static final String TEST_PASSWORD_5 = "klmnop";

  public static final String TEST_USER_6 = "Ldap\\, (Test6*)";
  public static final String TEST_PASSWORD_6 = "qrstu";

  public static final String TEST_USER_7 = "Test7Ldap";

  // TEST_USER_1 and TEST_USER_2 are members of this group.
  public static final String TEST_USER_GROUP = "group1";

  public static final String GROUP_DN_PATTERN = "cn=%s,ou=Groups,dc=myorg,dc=com";
}
