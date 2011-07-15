// Copyright (c) 2011 Cloudera, Inc. All rights reserved.

package com.cloudera.impala.service;

import java.util.concurrent.BlockingQueue;

import com.cloudera.impala.common.ImpalaException;
import com.cloudera.impala.thrift.TResultRow;

// Interface to C++ local executor
class NativePlanExecutor {
  protected native static void ExecPlan(byte[] thriftExecutePlanRequest,
      boolean asAscii, BlockingQueue<TResultRow> resultQueue) throws ImpalaException;

  static {
    // TODO: get this working:
    // System.loadLibrary("libplanexec");
    //System.load("/home/marcel/impala/be/build/service/libplanexec.so");
    //System.load("/home/abehm/hive/build/hadoopcore/hadoop-0.20.2-cdh3u1-SNAPSHOT/c++/Linux-amd64-64/lib/libhdfs.so");
    System.load("/home/abehm/impala/be/build/service/libplanexec.so");
  }
}

