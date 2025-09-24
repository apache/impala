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
package org.apache.impala.service.catalogmanager;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.impala.common.ImpalaRuntimeException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigLoader {

  private static final Logger LOG = LoggerFactory.getLogger(ConfigLoader.class);

  private final File configFolder_;
  private final UnaryOperator<String> envVarResolver_;

  public ConfigLoader(File configFolder) {
    this(configFolder, System::getenv);
  }

  public ConfigLoader(File configFolder, UnaryOperator<String> envVarResolver) {
    this.configFolder_ = configFolder;
    this.envVarResolver_ = envVarResolver;
  }

  public List<Properties> loadConfigs() throws ImpalaRuntimeException {
    List<File> files = listFiles(configFolder_);
    List<Properties> propertiesList = new ArrayList<>();
    for (File configFile : files) {
      String fileName = configFile.getName();
      try {
        Properties props = readPropertiesFile(configFile);
        checkPropertyValue(fileName, props, "connector.name", "iceberg");
        checkPropertyValue(fileName, props, "iceberg.catalog.type", "rest");
        propertiesList.add(props);
      } catch (IOException e) {
        throw new ImpalaRuntimeException(
            String.format("Unable to read file %s from configuration directory: %s",
                fileName, configFolder_.getAbsolutePath()), e);
      }
    }
    return propertiesList;
  }

  List<File> listFiles(File configDirectory) {
    Preconditions.checkState(configDirectory.exists() && configDirectory.isDirectory());
    return Stream.of(configDirectory.listFiles())
        .filter(file -> !file.isDirectory())
        .collect(Collectors.toList());
  }

  Properties readPropertiesFile(File file) throws IOException {
    Properties props = new Properties();
    try (InputStream in = Files.newInputStream(file.toPath())) {
      props.load(in);
    }
    this.resolveEnvVars(props);
    return props;
  }

  private void resolveEnvVars(Properties props) {
    for (String key : props.stringPropertyNames()) {
      String value = props.getProperty(key);
      String resolved = resolveEnvVar(value);
      props.setProperty(key, resolved);
    }
  }

  private String resolveEnvVar(String input) {
    if (input == null || input.isEmpty()) return input;
    Pattern pattern = Pattern.compile("\\$\\{ENV:([a-zA-Z][a-zA-Z0-9_-]*)}");
    Matcher matcher = pattern.matcher(input);
    StringBuffer sb = new StringBuffer();
    while (matcher.find()) {
      String envVarName = matcher.group(1);
      String envVarValue = this.envVarResolver_.apply(envVarName);
      String replacement;
      if (envVarValue == null) {
        LOG.error("Unable to resolve environment variable: '{}'", envVarName);
        replacement = matcher.group(0);
      }
      else {
        replacement = envVarValue;
      }
      matcher.appendReplacement(sb, Matcher.quoteReplacement(replacement));
    }
    matcher.appendTail(sb);
    return sb.toString();
  }

  private void checkPropertyValue(String configFile, Properties props, String key,
      String expectedValue) {
    if (!props.containsKey(key)) {
      throw new IllegalStateException(String.format(
          "Expected property %s was not specified in config file %s.", key,
          configFile));
    }
    String actualValue = props.getProperty(key);
    if (!Objects.equals(actualValue, expectedValue)) {
      throw new IllegalStateException(String.format(
          "Expected value of '%s' is '%s', but found '%s' in config file %s",
          key, expectedValue, actualValue, configFile));
    }
  }
}
