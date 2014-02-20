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

package org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair;

import com.google.common.base.Preconditions;

/**
 * Helper methods for AllocationFileLoaderService which need to be in the same package
 * to access package-private members for testing.
 * TODO: Remove and replace functionality with reflection.
 */
public class AllocationFileLoaderServiceHelper {

  /**
   * Set the package-private member reloadIntervalMs.
   */
  public static void setFileReloadIntervalMs(AllocationFileLoaderService service,
      long reloadIntervalMs) {
    Preconditions.checkNotNull(service);
    service.reloadIntervalMs = reloadIntervalMs;
  }
}
