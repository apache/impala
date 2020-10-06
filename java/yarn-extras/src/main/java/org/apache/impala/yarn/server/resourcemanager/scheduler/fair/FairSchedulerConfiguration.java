/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.impala.yarn.server.resourcemanager.scheduler.fair;
//YARNUTIL: MODIFIED

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.impala.yarn.server.utils.BuilderUtils;

public class FairSchedulerConfiguration extends Configuration {

  private static final String CONF_PREFIX = "yarn.scheduler.fair.";

  public static final String ALLOCATION_FILE = CONF_PREFIX + "allocation.file";
  protected static final String DEFAULT_ALLOCATION_FILE = "fair-scheduler.xml";

  /** Whether pools can be created that were not specified in the FS configuration file
   */
  protected static final String ALLOW_UNDECLARED_POOLS = CONF_PREFIX + "allow-undeclared-pools";
  protected static final boolean DEFAULT_ALLOW_UNDECLARED_POOLS = true;

  /**
   * Whether to use the user name as the queue name (instead of "default") if
   * the request does not specify a queue.
   */
  protected static final String USER_AS_DEFAULT_QUEUE = CONF_PREFIX + "user-as-default-queue";
  protected static final boolean DEFAULT_USER_AS_DEFAULT_QUEUE = true;

  /**
   * Parses a resource config value of a form like "1024", "1024 mb",
   * or "1024 mb, 3 vcores". If no units are given, megabytes are assumed.
   *
   * @throws AllocationConfigurationException
   */
  public static Resource parseResourceConfigValue(String val)
      throws AllocationConfigurationException {
    try {
      val = val.toLowerCase();
      int memory = findResource(val, "mb");
      int vcores = findResource(val, "vcores");
      return BuilderUtils.newResource(memory, vcores);
    } catch (AllocationConfigurationException ex) {
      throw ex;
    } catch (Exception ex) {
      throw new AllocationConfigurationException(
          "Error reading resource config", ex);
    }
  }

  private static int findResource(String val, String units)
    throws AllocationConfigurationException {
    Pattern pattern = Pattern.compile("(\\d+)(\\.\\d*)?\\s*" + units);
    Matcher matcher = pattern.matcher(val);
    if (!matcher.find()) {
      throw new AllocationConfigurationException("Missing resource: " + units);
    }
    return Integer.parseInt(matcher.group(1));
  }
}
