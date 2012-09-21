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
public class TrevniOutputFormat<K extends WritableComparable<K>, V extends Writable>
  extends HiveOutputFormatImpl<K,V> implements HiveOutputFormat<K,V> {

  @Override
  public RecordWriter getHiveRecordWriter(JobConf jc, Path finalOutPath,
      final Class<? extends Writable> valueClass, boolean isCompressed,
      Properties tableProperties, Progressable progress) throws IOException {
    return null;
  }

}
