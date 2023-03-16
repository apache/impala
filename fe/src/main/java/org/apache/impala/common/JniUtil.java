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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.StackTraceElement;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.lang.management.RuntimeMXBean;
import java.lang.management.ThreadMXBean;
import java.lang.management.ThreadInfo;
import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.impala.service.BackendConfig;
import org.apache.impala.thrift.TGetJMXJsonResponse;
import org.apache.impala.util.JMXJsonUtil;
import org.apache.thrift.TBase;
import org.apache.thrift.TSerializer;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

import com.google.common.base.Joiner;

import org.apache.impala.thrift.TGetJvmMemoryMetricsResponse;
import org.apache.impala.thrift.TGetJvmThreadsInfoRequest;
import org.apache.impala.thrift.TGetJvmThreadsInfoResponse;
import org.apache.impala.thrift.TJvmMemoryPool;
import org.apache.impala.thrift.TJvmThreadInfo;
import org.apache.impala.util.JvmPauseMonitor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class with methods intended for JNI clients
 */
public class JniUtil {
  private final static TBinaryProtocol.Factory protocolFactory_ =
      new TBinaryProtocol.Factory();

  private static final Logger LOG = LoggerFactory.getLogger(JniUtil.class);

  /**
   * Initializes the JvmPauseMonitor instance.
   */
  public static void initPauseMonitor(long deadlockCheckIntervalS) {
    JvmPauseMonitor.INSTANCE.initPauseMonitor(deadlockCheckIntervalS);
  }

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
   * Serializes input into a byte[] using the default protocol factory.
   */
  public static <T extends TBase<?, ?>>
  byte[] serializeToThrift(T input) throws ImpalaException {
    try {
      TSerializer serializer = new TSerializer(protocolFactory_);
      return serializer.serialize(input);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Serializes input into a byte[] using a given protocol factory.
   */
  public static <T extends TBase<?, ?>, F extends TProtocolFactory>
  byte[] serializeToThrift(T input, F protocolFactory) throws ImpalaException {
    try {
      TSerializer serializer = new TSerializer(protocolFactory);
      return serializer.serialize(input);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  public static <T extends TBase<?, ?>>
  void deserializeThrift(T result, byte[] thriftData) throws ImpalaException {
    deserializeThrift(protocolFactory_, result, thriftData);
  }

  /**
   * Deserialize a serialized form of a Thrift data structure to its object form.
   */
  public static <T extends TBase<?, ?>, F extends TProtocolFactory>
  void deserializeThrift(F protocolFactory, T result, byte[] thriftData)
      throws ImpalaException {
    // TODO: avoid creating deserializer for each query?
    try {
      TDeserializer deserializer = new TDeserializer(protocolFactory);
      deserializer.deserialize(result, thriftData);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  public static class OperationLog {
    private final long startTime;
    private final String methodName;
    private final String shortDescription;
    private final boolean silentStartAndFinish;

    public OperationLog(
        String methodName, String shortDescription, boolean silentStartAndFinish) {
      this.startTime = System.currentTimeMillis();
      this.methodName = methodName;
      this.shortDescription = shortDescription;
      this.silentStartAndFinish = silentStartAndFinish;
    }

    public void logStart() {
      final String startFormat = "{} request: {}";
      if (silentStartAndFinish) {
        LOG.trace(startFormat, methodName, shortDescription);
      } else {
        LOG.info(startFormat, methodName, shortDescription);
      }
    }

    public void logFinish() {
      final String finishFormat = "Finished {} request: {}. Time spent: {}";
      long duration = getDurationFromStart();
      String durationString = PrintUtils.printTimeMs(duration);
      if (silentStartAndFinish) {
        LOG.trace(finishFormat, methodName, shortDescription, durationString);
      } else {
        LOG.info(finishFormat, methodName, shortDescription, durationString);
      }
    }

    public void logError() {
      long duration = getDurationFromStart();
      LOG.error("Error in {}. Time spent: {}", shortDescription,
          PrintUtils.printTimeMs(duration));
    }

    /**
     * Warn if the result size or the response time exceeds thresholds.
     */
    public void logResponse(long resultSize, TBase<?, ?> thriftReq) {
      long duration = getDurationFromStart();
      boolean tooLarge =
          (resultSize > BackendConfig.INSTANCE.getWarnCatalogResponseSize());
      boolean tooSlow =
          (duration > BackendConfig.INSTANCE.getWarnCatalogResponseDurationMs());
      if (tooLarge || tooSlow) {
        String header = (tooLarge && tooSlow) ?
            "Response too large and too slow" :
            (tooLarge ? "Response too large" : "Response too slow");
        String request = (thriftReq == null) ?
            "" :
            ", request: " + StringUtils.abbreviate(thriftReq.toString(), 1000);
        LOG.warn("{}: size={} ({}), duration={}ms ({}), method: {}{}", header, resultSize,
            PrintUtils.printBytes(resultSize), duration, PrintUtils.printTimeMs(duration),
            methodName, request);
      }
    }

    private long getDurationFromStart() {
      return System.currentTimeMillis() - this.startTime;
    }
  }

  private static OperationLog logOperationInternal(
      String methodName, String shortDescription, boolean silentStartAndFinish) {
    OperationLog operationLog =
        new OperationLog(methodName, shortDescription, silentStartAndFinish);
    operationLog.logStart();
    return operationLog;
  }

  public static OperationLog logOperation(String methodName, String shortDescription) {
    return logOperationInternal(methodName, shortDescription, false);
  }

  public static OperationLog logOperationSilentStartAndFinish(
      String methodName, String shortDescription) {
    return logOperationInternal(methodName, shortDescription, true);
  }

  /**
   * Collect the JVM's memory statistics into a thrift structure for translation into
   * Impala metrics by the backend. A synthetic 'total' memory pool is included with
   * aggregate statistics for all real pools. Metrics for the JvmPauseMonitor
   * and Garbage Collection are also included.
   */
  public static byte[] getJvmMemoryMetrics() throws ImpalaException {
    TGetJvmMemoryMetricsResponse jvmMetrics = new TGetJvmMemoryMetricsResponse();
    jvmMetrics.setMemory_pools(new ArrayList<TJvmMemoryPool>());
    TJvmMemoryPool totalUsage = new TJvmMemoryPool();

    totalUsage.setName("total");
    jvmMetrics.getMemory_pools().add(totalUsage);

    for (MemoryPoolMXBean memBean: ManagementFactory.getMemoryPoolMXBeans()) {
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

      jvmMetrics.getMemory_pools().add(usage);
    }

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

    // Populate non-heap usage
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

    // Populate JvmPauseMonitor metrics
    jvmMetrics.setGc_num_warn_threshold_exceeded(
        JvmPauseMonitor.INSTANCE.getNumGcWarnThresholdExceeded());
    jvmMetrics.setGc_num_info_threshold_exceeded(
        JvmPauseMonitor.INSTANCE.getNumGcInfoThresholdExceeded());
    jvmMetrics.setGc_total_extra_sleep_time_millis(
        JvmPauseMonitor.INSTANCE.getTotalGcExtraSleepTime());

    // And Garbage Collector metrics
    long gcCount = 0;
    long gcTimeMillis = 0;
    for (GarbageCollectorMXBean bean : ManagementFactory.getGarbageCollectorMXBeans()) {
      gcCount += bean.getCollectionCount();
      gcTimeMillis += bean.getCollectionTime();
    }
    jvmMetrics.setGc_count(gcCount);
    jvmMetrics.setGc_time_millis(gcTimeMillis);

    return serializeToThrift(jvmMetrics, protocolFactory_);
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
        // The regular ThreadInfo.toString() method limits the depth of the stacktrace.
        // To get around this, we use the first line of the toString() output (which
        // contains non-stacktrace information) and then construct our own stacktrace
        // based on ThreadInfo.getStackTrace() information.
        StringBuffer customSummary = new StringBuffer();
        String regularSummary = threadInfo.toString();
        int firstNewlineIndex = regularSummary.indexOf("\n");
        // Keep only the first line from the regular summary
        customSummary.append(regularSummary.substring(0, firstNewlineIndex));
        customSummary.append("\n");
        // Append a full stack trace that mimics how jstack displays the stack
        // (with indentation and "at")
        for (StackTraceElement ste : threadInfo.getStackTrace()) {
          customSummary.append("\tat " + ste.toString() + "\n");
        }
        tThreadInfo.setSummary(customSummary.toString());
        tThreadInfo.setCpu_time_in_ns(threadBean.getThreadCpuTime(id));
        tThreadInfo.setUser_time_in_ns(threadBean.getThreadUserTime(id));
        tThreadInfo.setBlocked_count(threadInfo.getBlockedCount());
        tThreadInfo.setBlocked_time_in_ms(threadInfo.getBlockedTime());
        tThreadInfo.setIs_in_native(threadInfo.isInNative());
        response.addToThreads(tThreadInfo);
      }
    }
    return serializeToThrift(response, protocolFactory_);
  }

  public static byte[] getJMXJson() throws  ImpalaException {
    TGetJMXJsonResponse response = new TGetJMXJsonResponse(JMXJsonUtil.getJMXJson());
    return serializeToThrift(response, protocolFactory_);
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
