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


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.Properties;
import org.apache.impala.common.ImpalaRuntimeException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

public class ConfigLoaderTest {

  @Rule
  public TemporaryFolder tempFolder = new TemporaryFolder();

  private File tempDir;

  @Before
  public void setUp() throws Exception {
    tempDir = tempFolder.newFolder("config");
  }

  private File createConfigFile(String fileName, String content) throws IOException {
    File configFile = new File(tempDir, fileName);
    try (FileWriter writer = new FileWriter(configFile)) {
      writer.write(content);
    }
    return configFile;
  }

  @Test
  public void testLoadValidConfigs() throws Exception {
    createConfigFile("valid1.properties",
        "connector.name=iceberg\niceberg.catalog.type=rest\n");
    createConfigFile("valid2.properties",
        "connector.name=iceberg\niceberg.catalog.type=rest\n");

    ConfigLoader loader = new ConfigLoader(tempDir);
    List<Properties> configs = loader.loadConfigs();

    assertEquals(2, configs.size());
    assertEquals("iceberg", configs.get(0).getProperty("connector.name"));
    assertEquals("rest", configs.get(0).getProperty("iceberg.catalog.type"));
  }

  @Test
  public void testMissingConnectorNameThrows() throws IOException {
    createConfigFile("bad.properties", "iceberg.catalog.type=rest\n");

    ConfigLoader loader = new ConfigLoader(tempDir);

    try {
      loader.loadConfigs();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("connector.name"));
    } catch (ImpalaRuntimeException e) {
      fail("Expected IllegalStateException");
    }
  }

  @Test
  public void testIncorrectCatalogTypeThrows() throws IOException {
    createConfigFile("bad.properties",
        "connector.name=iceberg\niceberg.catalog.type=hive\n");

    ConfigLoader loader = new ConfigLoader(tempDir);

    try {
      loader.loadConfigs();
      fail("Expected IllegalStateException");
    } catch (IllegalStateException e) {
      assertTrue(e.getMessage().contains("Expected value of 'iceberg.catalog.type'"));
    } catch (ImpalaRuntimeException e) {
      fail("Expected IllegalStateException");
    }
  }

  @Test
  public void testUnreadableFileThrows() throws IOException {
    File unreadableFile = createConfigFile("unreadable.properties",
        "connector.name=iceberg\niceberg.catalog.type=rest\n");

    // Make the file unreadable (on Unix-like systems)
    assertTrue(unreadableFile.setReadable(false));

    ConfigLoader loader = new ConfigLoader(tempDir);

    try {
      loader.loadConfigs();
      fail("Expected ImpalaRuntimeException");
    } catch (ImpalaRuntimeException e) {
      assertTrue(e.getMessage().contains("Unable to read file"));
    }
  }

  @Test
  public void testEmptyDirectoryReturnsEmptyList() throws Exception {
    ConfigLoader loader = new ConfigLoader(tempDir);
    List<Properties> configs = loader.loadConfigs();
    assertTrue(configs.isEmpty());
  }
}