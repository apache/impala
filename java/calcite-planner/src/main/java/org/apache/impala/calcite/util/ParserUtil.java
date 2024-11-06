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

package org.apache.impala.calcite.util;

import org.apache.calcite.sql.parser.SqlParserUtil;

import static java.lang.Integer.parseInt;
import java.util.function.Predicate;

/**
 * Util functions for parser
 *
 * IMPALA NOTE: This was copied from SqlParserUtil from Calcite 1.37 with one
 * modification. If there is a regexp character following the
 * backslash, we remove the backslash. The rest of the method is
 * copied as/is. The change is noted in a comment below (search for IMPALA)
 */
public class ParserUtil {
  /**
   * replaceEscapeChars
   *
   * Converts the contents of a character literal  with escapes like those used
   * in the C programming language to the corresponding Java string
   * representation.
   *
   * <p>If the literal "{@code E'a\tc'}" occurs in the SQL source text, then
   * this method will be invoked with the string "{@code a\tc}" (4 characters)
   * and will return a Java string with the three characters 'a', TAB, 'b'.
   *
   * @param input String that contains C-style escapes
   * @return String with escapes converted into Java characters
   * @throws MalformedUnicodeEscape if input contains invalid unicode escapes
   */
  public static String replaceEscapedChars(String input)
      throws MalformedUnicodeEscape {
    // The implementation of this method is based on Crate's method
    // Literals.replaceEscapedChars.
    final int length = input.length();
    if (length <= 1) {
      return input;
    }
    final StringBuilder builder = new StringBuilder(length);
    int endIdx;
    for (int i = 0; i < length; i++) {
      char currentChar = input.charAt(i);
      if (currentChar == '\\' && i + 1 < length) {
        char nextChar = input.charAt(i + 1);
        switch (nextChar) {
        case 'b':
          builder.append('\b');
          i++;
          break;
        case 'f':
          builder.append('\f');
          i++;
          break;
        case 'n':
          builder.append('\n');
          i++;
          break;
        case 'r':
          builder.append('\r');
          i++;
          break;
        case 't':
          builder.append('\t');
          i++;
          break;
        case '\\':
        case '\'':
        // IMPALA NOTE: Impala also allows escaping of these regexp characters. We
        // just remove the backslash here
        case '.':
        case '+':
        case '*':
        case '?':
        case '^':
        case '$':
        case '(':
        case ')':
        case '[':
        case ']':
        case '{':
        case '}':
        case '|':
          builder.append(nextChar);
          i++;
          break;
        case 'u':
        case 'U':
          // handle unicode case
          final int charsToConsume = (nextChar == 'u') ? 4 : 8;
          if (i + 1 + charsToConsume >= length) {
            throw new MalformedUnicodeEscape(i);
          }
          endIdx =
              calculateMaxCharsInSequence(input, i + 2, charsToConsume,
                  SqlParserUtil::isHexDigit);
          if (endIdx != i + 2 + charsToConsume) {
            throw new MalformedUnicodeEscape(i);
          }
          builder.appendCodePoint(parseInt(input.substring(i + 2, endIdx), 16));
          i = endIdx - 1; // skip already consumed chars
          break;
        case 'x':
          // handle hex byte case - up to 2 chars for hex value
          endIdx =
              calculateMaxCharsInSequence(input, i + 2, 2,
                  SqlParserUtil::isHexDigit);
          if (endIdx > i + 2) {
            builder.appendCodePoint(parseInt(input.substring(i + 2, endIdx), 16));
            i = endIdx - 1; // skip already consumed chars
          } else {
            // hex sequence unmatched - output original char
            builder.append(nextChar);
            i++;
          }
          break;
        case '0':
        case '1':
        case '2':
        case '3':
          // handle octal case - up to 3 chars
          endIdx =
              calculateMaxCharsInSequence(input, i + 2,
                  2, // first char is already "consumed"
                  SqlParserUtil::isOctalDigit);
          builder.appendCodePoint(parseInt(input.substring(i + 1, endIdx), 8));
          i = endIdx - 1; // skip already consumed chars
          break;
        default:
          // non-valid escaped char sequence
           builder.append(currentChar);
        }
      } else {
        builder.append(currentChar);
      }
    }
    return builder.toString();
  }

  /**
   * Calculates the maximum number of consecutive characters of the
   * {@link CharSequence} argument, starting from {@code beginIndex}, that match
   * a given {@link Predicate}. The number of characters to match are either
   * capped from the {@code maxCharsToMatch} parameter or the sequence length.
   *
   * <p>Examples:
   * <pre>
   * {@code
   *    calculateMaxCharsInSequence("12345", 0, 2, Character::isDigit) -> 2
   *    calculateMaxCharsInSequence("12345", 3, 2, Character::isDigit) -> 5
   *    calculateMaxCharsInSequence("12345", 4, 2, Character::isDigit) -> 5
   * }
   * </pre>
   *
   * @return the index of the first non-matching character
   */
  private static int calculateMaxCharsInSequence(CharSequence seq,
      int beginIndex,
      int maxCharsToMatch,
      Predicate<Character> predicate) {
    int idx = beginIndex;
    final int end = Math.min(seq.length(), beginIndex + maxCharsToMatch);
    while (idx < end && predicate.test(seq.charAt(idx))) {
      idx++;
    }
    return idx;
  }
  /** Thrown by {@link #replaceEscapedChars(String)}. */
  public static class MalformedUnicodeEscape extends Exception {
    public final int i;

    MalformedUnicodeEscape(int i) {
      this.i = i;
    }
  }
}
