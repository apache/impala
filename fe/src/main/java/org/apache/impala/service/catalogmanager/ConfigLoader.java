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
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.impala.common.ImpalaRuntimeException;

public class ConfigLoader {

  private final File configFolder_;

  public ConfigLoader(File configFolder) {
    this.configFolder_ = configFolder;
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
    return props;
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
