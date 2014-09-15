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

package com.cloudera.impala.common;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryPoolMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;

import org.apache.thrift.TBase;
import org.apache.thrift.TSerializer;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocolFactory;

import com.cloudera.impala.thrift.TGetJvmMetricsRequest;
import com.cloudera.impala.thrift.TGetJvmMetricsResponse;
import com.cloudera.impala.thrift.TJvmMemoryPool;

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
    Writer output = new StringWriter();
    try {
      output.write(String.format("%s: %s", t.getClass().getSimpleName(),
          t.getMessage()));
      // Follow the chain of exception causes and print them as well.
      Throwable cause = t;
      while ((cause = cause.getCause()) != null) {
        output.write(String.format("\nCAUSED BY: %s: %s",
            cause.getClass().getSimpleName(), cause.getMessage()));
      }
    } catch (IOException e) {
      throw new Error(e);
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
    TSerializer serializer = new TSerializer(protocolFactory_);
    try {
      return serializer.serialize(jvmMetrics);
    } catch (TException e) {
      throw new InternalException(e.getMessage());
    }
  }

  /**
   * Get Java version and vendor information
   */
  public static String getJavaVersion() {
    StringBuilder sb = new StringBuilder();
    sb.append("Java Version Info: ");
    sb.append(System.getProperty("java.runtime.name"));
    sb.append(" (");
    sb.append(System.getProperty("java.runtime.version"));
    sb.append(")");
    return sb.toString();
  }
}
