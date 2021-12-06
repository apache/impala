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

package org.apache.impala.authorization;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * A class to authorize access to a storage handler URI.
 */
public class AuthorizableStorageHandlerUri extends Authorizable {
    private final String storageType_;
    private final String storageUri_;

    public AuthorizableStorageHandlerUri(String storageType, String storageUri) {
        Preconditions.checkArgument(!Strings.isNullOrEmpty(storageType));
        Preconditions.checkArgument(!Strings.isNullOrEmpty(storageUri));
        storageType_ = storageType;
        storageUri_ = storageUri;
    }

    @Override
    public String getName() {
      return storageType_ + "://" + storageUri_;
    }

    @Override
    public String getStorageType() {
      return storageType_;
    }

    @Override
    public String getStorageUri() {
      return storageUri_;
    }

    @Override
    public Type getType() { return Type.STORAGEHANDLER_URI; }
}
