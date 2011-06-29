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
    System.loadLibrary("libplanexec");
  }
}

