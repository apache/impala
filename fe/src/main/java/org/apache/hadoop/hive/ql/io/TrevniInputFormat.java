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

package org.apache.hadoop.hive.ql.io;

import java.io.IOException;
import java.util.List;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/** Dummy InputFormat Class so Hive will create Trevni files. */
public class TrevniInputFormat extends InputFormat<ImmutableBytesWritable, Result> {

  @Override
  public RecordReader<ImmutableBytesWritable, Result>
      createRecordReader(InputSplit arg0, TaskAttemptContext arg1)
      throws IOException, InterruptedException {
    return null;
  }

  @Override
  public List<InputSplit> getSplits(JobContext arg0) throws
     IOException, InterruptedException {
    return null;
  }

}
