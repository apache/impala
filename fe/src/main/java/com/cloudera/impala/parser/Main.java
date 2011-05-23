package com.cloudera.impala.parser;

import java_cup.runtime.SymbolFactory;
import java.io.StringReader;

class Main {
  public static void main(String args[]) throws Exception {
    StringReader sr = new StringReader("select * from bla");
    new SqlParser(new SqlScanner(sr)).parse();
    new SqlParser(new SqlScanner(new StringReader("select from bla"))).parse();
  }
}
