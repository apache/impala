package com.cloudera.impala.testutil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;

import com.cloudera.impala.catalog.PrimitiveType;

public class TestUtils {
  // Maps from uppercase type name to PrimitiveType
  private static Map<String, PrimitiveType> typeNameMap = new HashMap<String, PrimitiveType>();
  static {
    for(PrimitiveType type: PrimitiveType.values()) {
      typeNameMap.put(type.toString(), type);
    }
  }

  /**
   * Do a line-by-line comparison of actual and expected output.
   * Comparison of the individual lines ignores whitespace.
   *
   * @param actual
   * @param expected
   * @return an error message if actual does not match expected, "" otherwise.
   */
  public static String compareOutput(String[] actual, ArrayList<String> expected) {
    int mismatch = -1; // line w/ mismatch
    int maxLen = Math.min(actual.length, expected.size());
    for (int i = 0; i < maxLen; ++i) {
      // do a whitespace-insensitive comparison
      Scanner a = new Scanner(actual[i]);
      Scanner e = new Scanner(expected.get(i));
      while (a.hasNext() && e.hasNext()) {
        if (!a.next().equals(e.next())) {
          mismatch = i;
          break;
        }
      }
      if (mismatch != -1) {
        break;
      }
      if (a.hasNext() != e.hasNext()) {
        mismatch = i;
        break;
      }
    }
    if (mismatch == -1 && actual.length < expected.size()) {
      mismatch = actual.length;
    }

    if (mismatch != -1) {
      // print actual and expected, highlighting mismatch
      StringBuilder output =
          new StringBuilder("actual result doesn't match expected result:\n");
      for (int i = 0; i <= mismatch; ++i) {
        output.append(actual[i]).append("\n");
      }
      // underline mismatched line with "^^^..."
      for (int i = 0; i < actual[mismatch].length(); ++i) {
        output.append('^');
      }
      output.append("\n");
      for (int i = mismatch + 1; i < actual.length; ++i) {
        output.append(actual[i]).append("\n");
      }
      output.append("\nexpected:\n");
      for (String str : expected) {
        output.append(str).append("\n");
      }
      return output.toString();
    }

    if (actual.length > expected.size()) {
      // print actual and expected
      StringBuilder output =
          new StringBuilder("actual result contains extra output:\n");
      for (String str : actual) {
        output.append(str).append("\n");
      }
      output.append("\nexpected:\n");
      for (String str : expected) {
        output.append(str).append("\n");
      }
      return output.toString();
    }

    return "";
  }

  /**
   * Do an element-by-element comparison of actual and expected types.
   *
   * @param actual
   * @param expected
   * @return an error message if actual does not match expected, "" otherwise.
   */
  public static String compareOutputTypes(List<PrimitiveType> actual, String[] expectedStrTypes) {
    if (actual.size() != expectedStrTypes.length) {
      return "Unequal number of types. Found: " + actual.size() + ". Expected: "
          + expectedStrTypes.length;
    }
    for (int i = 0; i < expectedStrTypes.length; ++i) {
      String upperCaseTypeStr = expectedStrTypes[i].toUpperCase();
      PrimitiveType expectedType = typeNameMap.get(upperCaseTypeStr.trim());
      if (actual.get(i) != expectedType) {
        return "Slot: " + i + ". Found: " + actual.get(i).toString() + ". Expected: "
            + upperCaseTypeStr;
      }
    }
    return "";
  }
}
