// Copyright 2012 Cloudera Inc.
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

package com.cloudera.impala.catalog;

import com.cloudera.impala.thrift.TFileFormat;

public enum FileFormat {
  TEXTFILE("TEXTFILE", TFileFormat.TEXTFILE),
  SEQUENCEFILE("SEQUENCEFILE", TFileFormat.SEQUENCEFILE),
  RCFILE("RCFILE", TFileFormat.RCFILE);

  private final String description;
  private final TFileFormat thriftType;

  private FileFormat(String description, TFileFormat thriftType) {
    this.description = description;
    this.thriftType = thriftType;
  }

  public String getDescription() {
    return description;
  }

  public TFileFormat toThrift() {
    return thriftType;
  }

  public static FileFormat fromThrift(TFileFormat fileFormat) {
    switch (fileFormat) {
      case SEQUENCEFILE: return FileFormat.SEQUENCEFILE;
      case RCFILE: return FileFormat.RCFILE;
      case TEXTFILE: return FileFormat.TEXTFILE;
      default:
          throw new UnsupportedOperationException(
              "Unknown TFileFormat value: " + fileFormat);
    }
  }
}
