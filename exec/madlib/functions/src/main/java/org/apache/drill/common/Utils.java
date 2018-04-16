/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.common;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Objects;
import com.google.common.base.Preconditions;
import org.apache.velocity.util.StringUtils;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Random;

public class Utils {
  public static final String SCHEMA_TMP = "dfs.tmp";
  private static final ObjectMapper MAPPER = new ObjectMapper();

  static {
    MAPPER.configure(JsonParser.Feature.ALLOW_NUMERIC_LEADING_ZEROS, true);
    MAPPER.configure(JsonParser.Feature.ALLOW_NON_NUMERIC_NUMBERS, true);
  }

  public static String unique_string() {
    return unique_string("");
  }

  /**
   * Generate random remporary names for temp table and other names.
      It has a SQL interface so both SQL and Python functions can call it.
   * @param desp
   * @return return the unique string
   */
  public static String unique_string(String desp) {
    int time = (int) (System.currentTimeMillis() / 1000);
    Random r = new Random();
    int r1 = r.nextInt(100000000) + 1;
    int r2 = time;
    int r3 = r2 % (r.nextInt(100000000) + 1);
    return String.format("__madlib_temp_%s%d_%d_%d__", desp, r1, r2, r3);
  }

  /**
   * Split a string into an array of strings
   Any space around the substrings are removed

   Requirement: every individual element in the string
   must be a valid Postgres name, which means that if
   there are spaces or commas in the element then the
   whole element must be quoted by a pair of double
   quotes.

   Usually this is not a problem. Especially in older
   versions of GPDB, when an array is passed from
   SQL to Python, it is converted to a string, which
   automatically adds double quotes if there are spaces or
   commas in the element.

   So use this function, if you are sure that all elements
   are valid Drill names.
   * @param s string
   * @return return the array
   */
  public static String[] _string_to_array(String s) {
    String[] result = StringUtils.split(s, ",");
    for (int i = 0; i < result.length; i++) {
      result[i] = StringUtils.nullTrim(result[i]);
    }
    return result;
  }

  /**
   * Perform a series of substitutions. The substitions
   * are performed by replacing $variable in the target
   * string with the value of provided by the key "variable"
   * in the provided hashtable.
   *
   *
   * @param format target string
   * @param params name/value pairs used for substitution
   * @return String target string with replacements.
   */
  public static String _format_string_by_map(
    String format, Map<String, ?> params) {
    return StringUtils.stringSubstitution(format, params).toString();
  }

  /**
   * Add a postfix to a string
   * @param str the string to be added
   * @param postfix the postfix string
   * @return return the result
   */
  public static String add_postfix(String str, String postfix) {
    str = org.apache.commons.lang3.StringUtils.strip(str);
    if (str.startsWith("\"") && str.endsWith("\"")) {
      return org.apache.commons.lang3.StringUtils.stripEnd(str, "\"") + postfix + "\"";
    }
    return str+postfix;

  }

  /**
   * Determines if the specified {@link CharSequence} is permissible as a Unicode identifier.
   * @param cs the char sequence to be tested
   * @return {@code true} if the char sequence is a Unicode identifier; {@code false} otherwise.
   */
  public static boolean is_identifier(CharSequence cs) {
    if (cs == null || cs.length() == 0) {
      return false;
    }
    if (!Character.isUnicodeIdentifierStart(cs.charAt(0))) {
      return false;
    }
    int sz = cs.length();
    for (int i = 1; i < sz; i++) {
      if (!Character.isUnicodeIdentifierPart(cs.charAt(i))) {
        return false;
      }
    }
    return true;
  }

  /**
   * Add prefix for every element in array[]. e.g.:
   * array[a,b] -> array[prefix.a, prefix.b];
   * array[12,a] -> array[12, prefix.a]
   *
   * @param str the array string to be added
   * @param prefix the prefix string
   * @return the converted result
   */
  public static String add_prefix_in_array(String str, String prefix) {
    if (str == null || org.apache.commons.lang3.StringUtils.isBlank(str)) {
      return str;
    }
    str = org.apache.commons.lang3.StringUtils.strip(str);
    if (org.apache.commons.lang3.StringUtils.startsWith(str.toLowerCase(), "array[")) {
      str = org.apache.commons.lang3.StringUtils.substring(str, 6, str.length()-1);
      String[] arr = str.split(",");
      for (int i = 0; i < arr.length; i++) {
        String e = org.apache.commons.lang3.StringUtils.strip(arr[i]);
        if (is_identifier(e)) {
          arr[i] = prefix + e;
        }
      }
      return "array[" + org.apache.commons.lang3.StringUtils.join(arr, ',') + "]";
    } else {
      return str;
    }
  }

  /**
   * Add prefix for every element in columns. e.g.:
   * a,b -> prefix.a, prefix.b
   *
   * @param str the columns string to be added
   * @param prefix the prefix string
   * @return the converted result
   */
  public static String add_prefix_in_columns(String str, String prefix) {
    if (org.apache.commons.lang3.StringUtils.isBlank(str)) {
      return str;
    }
    str = org.apache.commons.lang3.StringUtils.strip(str);
    String[] arr = str.split(",");
    for (int i = 0; i < arr.length; i++) {
      String e = org.apache.commons.lang3.StringUtils.strip(arr[i]);
      if (is_identifier(e)) {
        arr[i] = prefix + e;
      }
    }
    return org.apache.commons.lang3.StringUtils.join(arr, ',');
  }

  public static <T> String list_to_sql_string(Collection<T> array, final boolean needQuotes) {
    String array_str = "array[%s]";
    Object elements = array.stream()
      .map(e->String.format(needQuotes?"'%s'" : "%s", e))
      .reduce((o, o2) -> String.format("%s, %s", o, o2)).orElse("");
    return String.format(array_str, elements);
  }

  /**
   * If the given condition is false, then throw an exception with the message.
   * @param condition the condition to be asserted
   * @param msg the error message to be reported
   */
  public static void _assert(Boolean condition, String msg) {
    Preconditions.checkState(condition, msg);
  }

  /**
   * If the given objects are not equal, then raise an error with the message.
   * @param o1 the first object
   * @param o2 the second object
   * @param msg the error message to be reported
   */
  public static void _assert_equal(Object o1, Object o2, String msg) {
    if (o1 != null) {
      Preconditions.checkState(Objects.equal(o1, o2), msg);
    }
  }

  public static String parse_to_json(Object obj) {
    try {

      return MAPPER.writeValueAsString(obj)
        .replaceAll("\"Infinity\"", "Infinity")
        .replaceAll("\"-Infinity\"", "-Infinity")
        .replaceAll("\"NaN\"", "NaN");
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  public static<T> T parse_from_json(String json, Class<T> _class) {
    try {
      return MAPPER.readValue(json, _class);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }
}
