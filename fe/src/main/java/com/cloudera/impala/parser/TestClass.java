package com.cloudera.impala.parser;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.builder.ToStringBuilder;

import com.google.common.base.Objects;

public class TestClass {
  public int i;
  public long l;

  @Override
  public int hashCode() {
    throw new NotImplementedException("Not implemented");
  }

  public String guavaDebugString() {
    return Objects.toStringHelper(this).add("i", i).toString();
  }

  public String javaDebugString() {
    return new ToStringBuilder(this).append("i", i).toString();
  }

  public static void main(String args[]) {
    TestClass c = new TestClass();

    String s = "abc";

    Object o = (s == null ? "abc" : s);
    boolean b = o instanceof String;
    System.err.println(b);

    String g = c.guavaDebugString();
    System.err.println(g);

    String j = c.javaDebugString();
    System.err.println(j);
  }
}
