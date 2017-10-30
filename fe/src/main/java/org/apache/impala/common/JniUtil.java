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

package org.apache.impala.common;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.lang.management.ThreadInfo;
import java.util.ArrayList;
import java.util.Map;

import org.apache.thrift.TBase;
import org.apache.thrift.TSerializer;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

import com.google.common.base.Joiner;

import org.apache.impala.thrift.TGetJvmMetricsRequest;
import org.apache.impala.thrift.TGetJvmMetricsResponse;
import org.apache.impala.thrift.TGetJvmThreadsInfoRequest;
import org.apache.impala.thrift.TGetJvmThreadsInfoResponse;
import org.apache.impala.thrift.TJvmMemoryPool;
import org.apache.impala.thrift.TJvmThreadInfo;

/**
 * Utility class with methods intended for JNI clients
 */
public class JniUtil {
  private final static TBinaryProtocol.Factory protocolFactory_ =
      new TBinaryProtocol.Factory();

  /**
   * Returns a formatted string containing the simple exception name and the
   * exception message without the full stack trace. Includes the
   * the chain of causes each in a separate line.
   */
  public static String throwableToString(Throwable t) {
    StringWriter output = new StringWriter();
    output.write(String.format("%s: %s", t.getClass().getSimpleName(),
        t.getMessage()));
    // Follow the chain of exception causes and print them as well.
    Throwable cause = t;
    while ((cause = cause.getCause()) != null) {
      output.write(String.format("\nCAUSED BY: %s: %s",
          cause.getClass().getSimpleName(), cause.getMessage()));
    }
    return output.toString();
  }

  /**
   * Returns the stack trace of the Throwable object.
   */
  public static String throwableToStackTrace(Throwable t) {
    Writer output = new StringWriter();
    t.printStackTrace(new PrintWriter(output));
    return output.toString();
  }

  /**
   * Deserialize a serialized form of a Thrift data structure to its object form.
   */
  public static <T extends TBase<?, ?>, F extends TProtocolFactory>
  void deserializeThrift(F protocolFactory, T result, byte[] thriftData)
      throws ImpalaException {
    // TODO: avoid creating deserializer for each query?
    TDeserializer deserializer = new TDeserializer(protocolFactory);
    try {
      deserializer.deserialize(result, thriftData);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Collect the JVM's memory statistics into a thrift structure for translation into
   * Impala metrics by the backend. A synthetic 'total' memory pool is included with
   * aggregate statistics for all real pools.
   */
  public static byte[] getJvmMetrics(byte[] argument) throws ImpalaException {
    TGetJvmMetricsRequest request = new TGetJvmMetricsRequest();
    JniUtil.deserializeThrift(protocolFactory_, request, argument);

    TGetJvmMetricsResponse jvmMetrics = new TGetJvmMetricsResponse();
    jvmMetrics.setMemory_pools(new ArrayList<TJvmMemoryPool>());
    TJvmMemoryPool totalUsage = new TJvmMemoryPool();
    boolean is_total =
        request.getMemory_pool() != null && request.getMemory_pool().equals("total");

    if (request.get_all || is_total) {
      totalUsage.setName("total");
      jvmMetrics.getMemory_pools().add(totalUsage);
    }
    for (MemoryPoolMXBean memBean: ManagementFactory.getMemoryPoolMXBeans()) {
      if (request.get_all || is_total ||
          memBean.getName().equals(request.getMemory_pool())) {
        TJvmMemoryPool usage = new TJvmMemoryPool();
        MemoryUsage beanUsage = memBean.getUsage();
        usage.setCommitted(beanUsage.getCommitted());
        usage.setInit(beanUsage.getInit());
        usage.setMax(beanUsage.getMax());
        usage.setUsed(beanUsage.getUsed());
        usage.setName(memBean.getName());

        totalUsage.committed += beanUsage.getCommitted();
        totalUsage.init += beanUsage.getInit();
        totalUsage.max += beanUsage.getMax();
        totalUsage.used += beanUsage.getUsed();

        MemoryUsage peakUsage = memBean.getPeakUsage();
        usage.setPeak_committed(peakUsage.getCommitted());
        usage.setPeak_init(peakUsage.getInit());
        usage.setPeak_max(peakUsage.getMax());
        usage.setPeak_used(peakUsage.getUsed());

        totalUsage.peak_committed += peakUsage.getCommitted();
        totalUsage.peak_init += peakUsage.getInit();
        totalUsage.peak_max += peakUsage.getMax();
        totalUsage.peak_used += peakUsage.getUsed();

        if (!is_total) {
          jvmMetrics.getMemory_pools().add(usage);
          if (!request.get_all) break;
        }
      }
    }

    if (request.get_all || request.getMemory_pool().equals("heap")) {
      // Populate heap usage
      MemoryMXBean mBean = ManagementFactory.getMemoryMXBean();
      TJvmMemoryPool heap = new TJvmMemoryPool();
      MemoryUsage heapUsage = mBean.getHeapMemoryUsage();
      heap.setCommitted(heapUsage.getCommitted());
      heap.setInit(heapUsage.getInit());
      heap.setMax(heapUsage.getMax());
      heap.setUsed(heapUsage.getUsed());
      heap.setName("heap");
      heap.setPeak_committed(0);
      heap.setPeak_init(0);
      heap.setPeak_max(0);
      heap.setPeak_used(0);
      jvmMetrics.getMemory_pools().add(heap);
    }

    if (request.get_all || request.getMemory_pool().equals("non-heap")) {
      // Populate non-heap usage
      MemoryMXBean mBean = ManagementFactory.getMemoryMXBean();
      TJvmMemoryPool nonHeap = new TJvmMemoryPool();
      MemoryUsage nonHeapUsage = mBean.getNonHeapMemoryUsage();
      nonHeap.setCommitted(nonHeapUsage.getCommitted());
      nonHeap.setInit(nonHeapUsage.getInit());
      nonHeap.setMax(nonHeapUsage.getMax());
      nonHeap.setUsed(nonHeapUsage.getUsed());
      nonHeap.setName("non-heap");
      nonHeap.setPeak_committed(0);
      nonHeap.setPeak_init(0);
      nonHeap.setPeak_max(0);
      nonHeap.setPeak_used(0);
      jvmMetrics.getMemory_pools().add(nonHeap);
    }

    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
      return serializer.serialize(jvmMetrics);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Get information about the live JVM threads.
   */
  public static byte[] getJvmThreadsInfo(byte[] argument) throws ImpalaException {
    TGetJvmThreadsInfoRequest request = new TGetJvmThreadsInfoRequest();
    JniUtil.deserializeThrift(protocolFactory_, request, argument);
    TGetJvmThreadsInfoResponse response = new TGetJvmThreadsInfoResponse();
    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
    response.setTotal_thread_count(threadBean.getThreadCount());
    response.setDaemon_thread_count(threadBean.getDaemonThreadCount());
    response.setPeak_thread_count(threadBean.getPeakThreadCount());
    if (request.get_complete_info) {
      for (ThreadInfo threadInfo: threadBean.dumpAllThreads(true, true)) {
        TJvmThreadInfo tThreadInfo = new TJvmThreadInfo();
        long id = threadInfo.getThreadId();
        tThreadInfo.setSummary(threadInfo.toString());
        tThreadInfo.setCpu_time_in_ns(threadBean.getThreadCpuTime(id));
        tThreadInfo.setUser_time_in_ns(threadBean.getThreadUserTime(id));
        tThreadInfo.setBlocked_count(threadInfo.getBlockedCount());
        tThreadInfo.setBlocked_time_in_ms(threadInfo.getBlockedTime());
        tThreadInfo.setIs_in_native(threadInfo.isInNative());
        response.addToThreads(tThreadInfo);
      }
    }

    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
      return serializer.serialize(response);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Get Java version, input arguments and system properties.
   */
  public static String getJavaVersion() {
    RuntimeMXBean runtime = ManagementFactory.getRuntimeMXBean();
    StringBuilder sb = new StringBuilder();
    sb.append("Java Input arguments:\n");
    sb.append(Joiner.on(" ").join(runtime.getInputArguments()));
    sb.append("\nJava System properties:\n");
    for (Map.Entry<String, String> entry: runtime.getSystemProperties().entrySet()) {
      sb.append(entry.getKey() + ":" + entry.getValue() + "\n");
    }
    return sb.toString();
  }
}
