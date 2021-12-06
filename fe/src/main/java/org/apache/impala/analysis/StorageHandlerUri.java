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

package org.apache.impala.analysis;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.impala.common.AnalysisException;

import com.google.common.collect.ImmutableSet;

public class StorageHandlerUri {
  public static final ImmutableSet<String> supportedStorageTypes = ImmutableSet.of(
      "kudu");

  private String storageType_;
  private String storageUrl_;

  public StorageHandlerUri(String storageHandlerUri) throws AnalysisException,
      URISyntaxException {
    // We consider "*://*" as a valid URI since it is supported by Ranger.
    if (storageHandlerUri.equals("*://*")) {
      storageType_ = "*";
      storageUrl_ = "*";
      return;
    }

    // We consider "<storage_type>://*" as a valid URI since it is supported by Ranger.
    String res[] = storageHandlerUri.split(":\\/\\/", 2);
    if (res.length == 2) {
      if (supportedStorageTypes.contains(res[0].toLowerCase()) &&
          res[1].equals("*")) {
        storageType_ = res[0];
        storageUrl_ = "*";
        return;
      }
    }

    URI uri = new URI(storageHandlerUri);
    if (!supportedStorageTypes.contains(uri.getScheme())) {
      throw new AnalysisException("The storage type \"" + uri.getScheme() +
          "\" is not supported. A storage handler URI should be in the form of " +
          "<storage_type>://<hostname>[:<port>]/<path_to_resource>.");
    }

    if (uri.getHost() == null) {
      throw new AnalysisException("A storage handler URI should be in the form of " +
          "<storage_type>://<hostname>[:<port>]/<path_to_resource>.");
    }

    storageType_ = uri.getScheme();
    storageUrl_ = uri.getHost() + (uri.getPort() == -1 ? "" : ":" + uri.getPort())
        + uri.getPath();
  }

  String getStorageType() {
    return storageType_;
  }

  String getStoreUrl() {
    return storageUrl_;
  }
}
