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

package org.apache.impala.util;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.impala.common.AnalysisException;
import org.apache.impala.common.FileSystemUtil;

/**
 * Provides common utilities for ORCSchemeExtractor and ParquetSchemeExtractor.
 */
public class FileAnalysisUtil {

  /**
   * Throws if the given path is not a file.
   */
  public static void CheckIfFile(Path pathToFile) throws AnalysisException {
    try {
      if (!FileSystemUtil.isFile(pathToFile)) {
        throw new AnalysisException("Cannot infer schema, path is not a file: " +
            pathToFile);
      }
    } catch (FileNotFoundException e) {
      throw new AnalysisException("Cannot infer schema, path does not exist: " +
          pathToFile);
    } catch (IOException e) {
      throw new AnalysisException("Failed to connect to filesystem:" + e);
    } catch (IllegalArgumentException e) {
      throw new AnalysisException(e.getMessage());
    }
  }
}
