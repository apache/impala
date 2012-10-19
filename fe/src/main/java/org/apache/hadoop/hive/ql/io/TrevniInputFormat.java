// Copyright (c) 2012 Cloudera, Inc. All rights reserved.
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
