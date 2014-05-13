// Copyright 2014 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.cloudera.impala.authorization;

import java.io.File;
import java.net.MalformedURLException;

import org.apache.hadoop.conf.Configuration;

import com.cloudera.impala.common.FileSystemUtil;
import com.google.common.base.Preconditions;

/**
 * Class used to load a sentry-site.xml configuration file for the Sentry
 * Policy Server.
 */
public class SentryServiceConfig {
  // Absolute path to the sentry-site.xml configuration file.
  private final String configFile_;

  // The Sentry Service configuration. Valid only after calling loadConfig().
  private Configuration config_;

  public SentryServiceConfig(String configFilePath) {
    Preconditions.checkNotNull(configFilePath);
    configFile_ = configFilePath;
  }

  public void loadConfig() {
    config_ = FileSystemUtil.getConfiguration();
    File configFile = new File(configFile_);
    if (!configFile.exists()) {
      throw new RuntimeException("Sentry Service configuration file does not exist: " +
          configFile_);
    }

    if (!configFile.canRead()) {
      throw new RuntimeException("Cannot read Sentry Service configuration file: " +
          configFile_);
    }

    // Load the config.
    try {
      config_.addResource(configFile.toURI().toURL());
    } catch (MalformedURLException e) {
      throw new RuntimeException("Invalid Sentry Service config file path: " +
          configFile_, e);
    }
  }

  public Configuration getConfig() { return config_; }
}