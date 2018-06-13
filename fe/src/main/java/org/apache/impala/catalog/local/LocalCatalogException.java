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

package org.apache.impala.catalog.local;

/**
 * Exception indicating an error loading catalog information into
 * the impalad.
 *
 * TODO(todd): should this be changed to a checked exception? The frontend
 * interfaces don't have appropriate "throws" clauses to do so without some
 * significant virality.
 */
public class LocalCatalogException extends RuntimeException {
  private static final long serialVersionUID = -429271377417249918L;

  public LocalCatalogException(String msg, Throwable cause) {
    super(msg, cause);
  }

  public LocalCatalogException(Throwable cause) {
    super(cause);
  }

  public LocalCatalogException(String msg) {
    super(msg);
  }
}
