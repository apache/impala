// Copyright (c) 2011 Cloudera, Inc. All rights reserved.
package org.apache.hadoop.hive.ql.io;

import java.io.IOException;
import java.util.Properties;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.Progressable;

/** Dummy Output Format Class so Hive will create Trevni files. */
public class TrevniOutputFormat
  implements HiveOutputFormat<WritableComparable, Writable> {

  @Override
  public RecordWriter getHiveRecordWriter(JobConf arg0, Path arg1,
      Class<? extends Writable> arg2,
      boolean arg3, Properties arg4, Progressable arg5) throws IOException {
    return null;
  }

}
