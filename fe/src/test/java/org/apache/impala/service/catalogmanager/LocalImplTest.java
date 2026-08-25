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
import static org.junit.Assert.assertNull;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.junit.Test;

public class LocalImplTest {
  @Test
  public void testDmlCatalogNamesRequireUniqueNonEmptyNames() {
    List<String> ids = LocalImpl.getIcebergDmlCatalogNames(Arrays.asList(
        restCatalog("duplicate"), restCatalog(""), restCatalog("duplicate"),
        restCatalog("unique")));

    assertEquals(4, ids.size());
    assertNull(ids.get(0));
    assertNull(ids.get(1));
    assertNull(ids.get(2));
    assertEquals("unique", ids.get(3));
  }

  private Properties restCatalog(String name) {
    Properties properties = new Properties();
    properties.setProperty("iceberg.rest-catalog.uri", "http://localhost:9084");
    if (!name.isEmpty()) properties.setProperty("iceberg.rest-catalog.name", name);
    return properties;
  }
}
