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

package org.apache.impala.authorization.sentry;

import java.io.File;
import java.net.MalformedURLException;

import org.apache.hadoop.conf.Configuration;

import org.apache.impala.common.FileSystemUtil;
import com.google.common.base.Strings;

/**
 * Class used to load a sentry-site.xml configuration file.
 */
public class SentryConfig {
  // Absolute path to the sentry-site.xml configuration file.
  private final String configFile_;

  // The Sentry configuration. Valid only after calling loadConfig().
  private final Configuration config_;

  public SentryConfig(String configFilePath) {
    configFile_ = configFilePath;
    config_ = FileSystemUtil.getConfiguration();
  }

  /**
   * Initializes the Sentry configuration.
   */
  public void loadConfig() {
    if (Strings.isNullOrEmpty(configFile_)) {
      throw new IllegalArgumentException("A valid path to a sentry-site.xml config " +
          "file must be set using --sentry_config to enable authorization.");
    }

    File configFile = new File(configFile_);
    if (!configFile.exists()) {
      String configFilePath = "\"" + configFile_ + "\"";
      throw new RuntimeException("Sentry configuration file does not exist: " +
          configFilePath);
    }

    if (!configFile.canRead()) {
      throw new RuntimeException("Cannot read Sentry configuration file: " +
          configFile_);
    }

    // Load the config.
    try {
      config_.addResource(configFile.toURI().toURL());
    } catch (MalformedURLException e) {
      throw new RuntimeException("Invalid Sentry config file path: " + configFile_, e);
    }
  }

  public Configuration getConfig() { return config_; }
  public String getConfigFile() { return configFile_; }
}
