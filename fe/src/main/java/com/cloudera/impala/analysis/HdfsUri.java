// Copyright 2013 Cloudera Inc.
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

package com.cloudera.impala.analysis;

import java.io.IOException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;

import com.cloudera.impala.authorization.AuthorizeableUri;
import com.cloudera.impala.authorization.Privilege;
import com.cloudera.impala.authorization.PrivilegeRequest;
import com.cloudera.impala.common.AnalysisException;
import com.cloudera.impala.common.FileSystemUtil;
import com.google.common.base.Preconditions;

/*
 * Represents an HDFS URI in a SQL statement.
 */
public class HdfsUri {
  private final String location_;

  // Set during analysis
  private Path uriPath_;

  public HdfsUri(String location) {
    Preconditions.checkNotNull(location);
    this.location_ = location.trim();
  }

  public Path getPath() {
    Preconditions.checkNotNull(uriPath_);
    return uriPath_;
  }

  public void analyze(Analyzer analyzer, Privilege privilege)
      throws AnalysisException {
    if (location_.isEmpty()) {
      throw new AnalysisException("URI path cannot be empty.");
    }

    uriPath_ = new Path(location_);
    if (!uriPath_.isUriPathAbsolute()) {
      throw new AnalysisException("URI path must be absolute: " + uriPath_);
    }
    try {
      FileSystem fs = uriPath_.getFileSystem(FileSystemUtil.getConfiguration());
      if (!(fs instanceof DistributedFileSystem)) {
        throw new AnalysisException(String.format("URI location '%s' " +
            "must point to an HDFS file system.", uriPath_));
      }
    } catch (IOException e) {
      throw new AnalysisException(e.getMessage(), e);
    }

    // Fully-qualify the path
    uriPath_ = FileSystemUtil.createFullyQualifiedPath(uriPath_);
    analyzer.registerPrivReq(new PrivilegeRequest(
        new AuthorizeableUri(uriPath_.toString()), privilege));
  }

  @Override
  public String toString() {
    // If uriPath is null (this HdfsURI has not been analyzed yet) just return the raw
    // location string the caller passed in.
    return uriPath_ == null ? location_ : uriPath_.toString();
  }

  public String getLocation() { return location_; }
}
