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

package org.apache.impala.authentication.saml;

import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import java.util.List;

import org.apache.impala.service.BackendConfig;
import org.pac4j.saml.credentials.SAML2Credentials.SAMLAttribute;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

// slightly modified (config replaced) version of
// https://github.com/vihangk1/hive/blob/master_saml/service/src/java/org/apache/hive/service/auth/saml/HiveSamlGroupNameFilter.java
public class HiveSamlGroupNameFilter implements Predicate<SAMLAttribute> {

  private static final Logger LOG = LoggerFactory
      .getLogger(HiveSamlGroupNameFilter.class);
  private final List<String> groupNames;
  private static final Splitter COMMA_SPLITTER = Splitter.on(',').trimResults()
      .omitEmptyStrings();
  private final String attributeName;

  public HiveSamlGroupNameFilter() {
    BackendConfig conf = BackendConfig.INSTANCE;
    String groupNameStr = conf.getSaml2GroupFilter();
    attributeName = conf.getSaml2GroupAttibuteName();
    ImmutableList.Builder<String> builder = ImmutableList.builder();
    if (groupNameStr != null && !groupNameStr.isEmpty()) {
      builder
          .addAll(COMMA_SPLITTER.split(groupNameStr));
    }
    groupNames = builder.build();
    LOG.debug("Initialized allowed group names as {}", groupNames);
  }

  public boolean apply(List<SAMLAttribute> attributes) {
    if (attributeName.isEmpty() && attributes.size() == 0) {
      return true;
    }
    for (SAMLAttribute attribute : attributes) {
      if (apply(attribute)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public boolean apply(SAMLAttribute attribute) {
    if (attributeName.isEmpty()) {
      // if attributeName is not configured, then it means groups based
      // filtering is not enabled and we allow any authenticated user.
      return true;
    }
    if (attribute == null || attribute.getName() == null) {
      return false;
    }
    if (!attributeName.equals(attribute.getName())) {
      LOG.debug("Attribute name {} did not match with {}", attribute.getName(),
          attributeName);
      return false;
    }
    for (String attrVal : attribute.getAttributeValues()) {
      LOG.debug("Evaluating group name {}", attrVal);
      if (groupNames.contains(attrVal)) {
        return true;
      }
    }
    return false;
  }
}