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
package org.apache.drill.madlib.common;

import com.google.common.base.Charsets;
import io.netty.buffer.DrillBuf;
import org.apache.drill.exec.expr.fn.impl.StringFunctionHelpers;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.BitHolder;
import org.apache.drill.exec.expr.holders.Float4Holder;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.RepeatedBigIntHolder;
import org.apache.drill.exec.expr.holders.RepeatedFloat4Holder;
import org.apache.drill.exec.expr.holders.RepeatedFloat8Holder;
import org.apache.drill.exec.expr.holders.RepeatedIntHolder;
import org.apache.drill.exec.expr.holders.RepeatedSmallIntHolder;
import org.apache.drill.exec.expr.holders.RepeatedTinyIntHolder;
import org.apache.drill.exec.expr.holders.SmallIntHolder;
import org.apache.drill.exec.expr.holders.VarBinaryHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;

import java.util.function.Consumer;


public class DataHolderUtils {

  /**
   * Extracting varchar value from {@link VarCharHolder}.
   * @param holder varchar value holder
   * @return return the varchar value
   */
  public static String extractVarchar(VarCharHolder holder) {
    return StringFunctionHelpers.getStringFromVarCharHolder(holder);
  }

  /**
   * Extracting varchar value from {@link NullableVarCharHolder}.
   * @param holder varchar value holder
   * @return return the varchar value
   */
  public static String extractVarchar(NullableVarCharHolder holder) {
    return StringFunctionHelpers.getStringFromVarCharHolder(holder);
  }
  /**
   * Extracting varchar value from {@link DrillBuf}.
   * @param buf drill buffer
   * @return return the varchar value
   */
  public static String extractVarchar(DrillBuf buf, int index) {
    int length = buf.getInt(index);
    return StringFunctionHelpers.toStringFromUTF8(index + 4, length, buf);
  }
  /**
   * Extracting varchar value from {@link DrillBuf}.
   * @param buf drill buffer
   * @return return the varchar value
   */
  public static String extractVarchar(DrillBuf buf) {
    return extractVarchar(buf, 0);
  }

  /**
   * Extracting integer value from {@link DrillBuf}.
   * @param buf drill buffer
   * @return return the integer value
   */
  public static Integer extractInt(DrillBuf buf, int index) {
    return buf.getInt(index);
  }

  /**
   * Extracting integer value from {@link DrillBuf}.
   * @param buf drill buffer
   * @return return the integer value
   */
  public static Integer extractInt(DrillBuf buf) {
    return extractInt(buf, 0);
  }

  /**
   * Extracting small integer value from {@link DrillBuf}.
   * @param buf drill buffer
   * @return return the smallint value
   */
  public static Short extractSmallint(DrillBuf buf, int index) {
    return buf.getShort(index);
  }

  /**
   * Extracting small integer value from {@link DrillBuf}.
   * @param buf drill buffer
   * @return return the smallint value
   */
  public static Short extractSmallint(DrillBuf buf) {
    return extractSmallint(buf, 0);
  }

  /**
   * Extracting big integer value from {@link DrillBuf}.
   * @param buf drill buffer
   * @return return the bigint value
   */
  public static Long extractBigint(DrillBuf buf, int index) {
    return buf.getLong(index);
  }

  /**
   * Extracting big integer value from {@link DrillBuf}.
   * @param buf drill buffer
   * @return return the bigint value
   */
  public static Long extractBigint(DrillBuf buf) {
    return extractBigint(buf, 0);
  }

  /**
   * Extracting big float4 value from {@link DrillBuf}.
   * @param buf drill buffer
   * @return return the float4 value
   */
  public static Float extractFloat4(DrillBuf buf, int index) {
    return buf.getFloat(index);
  }

  /**
   * Extracting big float4 value from {@link DrillBuf}.
   * @param buf drill buffer
   * @return return the float4 value
   */
  public static Float extractFloat4(DrillBuf buf) {
    return extractFloat4(buf, 0);
  }

  /**
   * Extracting big float8 value from {@link DrillBuf}.
   * @param buf drill buffer
   * @return return the float8 value
   */
  public static Double extractFloat8(DrillBuf buf, int index) {
    return buf.getDouble(index);
  }

  /**
   * Extracting big float8 value from {@link DrillBuf}.
   * @param buf drill buffer
   * @return return the float8 value
   */
  public static Double extractFloat8(DrillBuf buf) {
    return extractFloat8(buf, 0);
  }

  /**
   * Extracting big boolean value from {@link DrillBuf}.
   * @param buf drill buffer
   * @return return the boolean value
   */
  public static Boolean extractBoolean(DrillBuf buf, int index) {
    return buf.getInt(index) != 0;
  }

  /**
   * Extracting big boolean value from {@link DrillBuf}.
   * @param buf drill buffer
   * @return return the boolean value
   */
  public static Boolean extractBoolean(DrillBuf buf) {
    return extractBoolean(buf, 0);
  }

  /**
   * Copy varchar value from {@link VarCharHolder} to another {@link VarCharHolder}
   * @param from the varchar value holder will be copied
   * @param to the varchar value holder will copy to
   */
  public static void copyVarchar(VarCharHolder from, VarCharHolder to, DrillBuf drillBuf) {
    byte[] buf = new byte[from.end - from.start];
    from.buffer.getBytes(from.start, buf, 0, from.end - from.start);
    to.buffer = drillBuf.reallocIfNeeded(buf.length);
    to.end = to.start+buf.length;
    to.buffer.setBytes(to.start, buf, 0, buf.length);
  }

  /**
   * Copy varchar value from {@link VarCharHolder} to {@link DrillBuf}
   * @param from the varchar value holder will be copied
   * @param to the drill buffer holder will copy to
   */
  public static DrillBuf copyVarchar(VarCharHolder from, DrillBuf to) {
    int length = from.end - from.start;
    byte[] buf = new byte[length];
    from.buffer.getBytes(from.start, buf, 0, length);
    DrillBuf buffer = to.reallocIfNeeded(length+4);
    buffer.setInt(0, length);
    buffer.setBytes(4, buf, 0, buf.length);
    return buffer;
  }

  /**
   * Copy varchar value from {@link IntHolder} to {@link DrillBuf}
   * @param from the value holder will be copied
   * @param to the drill buffer holder will copy to
   */
  public static DrillBuf copyInt(IntHolder from, DrillBuf to) {
    DrillBuf buffer = to.reallocIfNeeded(4);
    buffer.setInt(0, from.value);
    return buffer;
  }

  /**
   * Copy varchar value from {@link BigIntHolder} to {@link DrillBuf}
   * @param from the value holder will be copied
   * @param to the drill buffer holder will copy to
   */
  public static DrillBuf copyBigint(BigIntHolder from, DrillBuf to) {
    DrillBuf buffer = to.reallocIfNeeded(8);
    buffer.setLong(0, from.value);
    return buffer;
  }

  /**
   * Copy varchar value from {@link Float4Holder} to {@link DrillBuf}
   * @param from the value holder will be copied
   * @param to the drill buffer holder will copy to
   */
  public static DrillBuf copyFloat4(Float4Holder from, DrillBuf to) {
    DrillBuf buffer = to.reallocIfNeeded(4);
    buffer.setFloat(0, from.value);
    return buffer;
  }

  /**
   * Copy varchar value from {@link Float8Holder} to {@link DrillBuf}
   * @param from the value holder will be copied
   * @param to the drill buffer holder will copy to
   */
  public static DrillBuf copyFloat8(Float8Holder from, DrillBuf to) {
    DrillBuf buffer = to.reallocIfNeeded(8);
    buffer.setDouble(0, from.value);
    return buffer;
  }

  /**
   * Copy varchar value from {@link BitHolder} to {@link DrillBuf}
   * @param from the value holder will be copied
   * @param to the drill buffer holder will copy to
   */
  public static DrillBuf copyBit(BitHolder from, DrillBuf to) {
    DrillBuf buffer = to.reallocIfNeeded(4);
    buffer.setInt(0, from.value);
    return buffer;
  }

  /**
   * Copy varchar value from {@link SmallIntHolder} to {@link DrillBuf}
   * @param from the value holder will be copied
   * @param to the drill buffer holder will copy to
   */
  public static DrillBuf copySmallInt(SmallIntHolder from, DrillBuf to) {
    DrillBuf buffer = to.reallocIfNeeded(2);
    buffer.setShort(0, from.value);
    return buffer;
  }

  /**
   * Extracting byte array value from {@link VarCharHolder}
   * @param holder varchar value holder
   * @return return the byte array
   */
  public static byte[] extractBytes(VarCharHolder holder) {
    byte[] buf = new byte[holder.end - holder.start];
    holder.buffer.getBytes(holder.start, buf, 0, holder.end - holder.start);
    return buf;
  }

  /**
   * Extracting byte array value from {@link VarBinaryHolder}
   * @param holder varchar value holder
   * @return return the byte array
   */
  public static byte[] extractBytes(VarBinaryHolder holder) {
    byte[] buf = new byte[holder.end - holder.start];
    holder.buffer.getBytes(holder.start, buf, 0, holder.end - holder.start);
    return buf;
  }

  /**
   * Extracting boolean value from {@link BitHolder}
   * @param holder bit value holder
   * @return return the boolean value
   */
  public static boolean extractBoolean(BitHolder holder) {
    return holder.value != 0;
  }

  /**
   * Extracting integer value from {@link IntHolder}
   * @param holder the integer value holder
   * @return return the integer value
   */
  public static int extractInteger(IntHolder holder) {
    return holder.value;
  }

  /**
   * Extracting long value from {@link BigIntHolder}
   * @param holder the bigint value holder
   * @return return the long value
   */
  public static long extractLong(BigIntHolder holder) {
    return holder.value;
  }

  /**
   * Assign byte array to the specific {@link DrillBuf}.
   * @param buf the buffer to be assigned to
   * @param data the byte array data
   * @return return the buffer
   */
  public static DrillBuf setBytes(DrillBuf buf, byte[] data) {
    DrillBuf newBuf = buf.reallocIfNeeded(data.length+4);
    newBuf.setInt(0, data.length);
    newBuf.setBytes(4, data);
    return newBuf;
  }

  /**
   * Get bytes from the specific {@link DrillBuf}.
   * @param buf the buffer to be got
   */
  public static byte[] getBytes(DrillBuf buf) {
    int length = buf.getInt(0);
    byte[] data = new byte[length];
    buf.getBytes(4, data, 0, length);
    return data;
  }

  /**
   * Set varchar value to output variable.
   * @param buf buffer
   * @param out the output varchar holder
   * @param result the varchar value to be set
   */
  public static void resetVarcharOut(DrillBuf buf, VarCharHolder out, String
    result) {
    byte[] data = result.getBytes(Charsets.UTF_8);
    int len = data.length;
    out.buffer = buf.reallocIfNeeded(len);
    out.end = len;
    out.start = 0;
    out.buffer.setBytes(out.start, data, 0, len);
  }

  /**
   * Set byte array value to output variable
   * @param buf buffer
   * @param out the output varchar holder
   * @param data the byte array value to be set
   */
  public static void resetVarcharOut(DrillBuf buf, VarCharHolder out, byte[] data) {
    int len = data.length;
    out.buffer = buf.reallocIfNeeded(len);
    out.start = 0;
    out.end = len;
    out.buffer.setBytes(out.start, data, 0, len);
  }

  /**
   * Set byte array value to intermediate variable
   * @param buf buffer
   * @param holder the binary intermediate varchar holder
   * @param data the byte array value to be set
   */
  public static void setBinary(DrillBuf buf, VarBinaryHolder holder, byte[] data) {
    int len = data.length;
    holder.buffer = buf.reallocIfNeeded(len);
    holder.start = 0;
    holder.end = len;
    holder.buffer.setBytes(holder.start, data, 0, len);
  }

  /**
   * Set double array value to intermediate variable
   * @param buf buffer
   * @param holder the binary intermediate varchar holder
   * @param data the byte array value to be set
   */
  public static void setBinary(DrillBuf buf, VarBinaryHolder holder, double[] data) {
    int len = data.length;
    holder.buffer = buf.reallocIfNeeded(len*8);
    holder.start = 0;
    holder.end = len*8;

    for (int i = 0; i < data.length; i++) {
      holder.buffer.setDouble(i*8, data[i]);
    }

  }

  /**
   * Extracting two-dimensions double array from {@link RepeatedIntHolder}.
   * @param h the repeated int value holder
   * @return return the two-dimensions double array
   */
  public static double[][] extract2DoubleArray(RepeatedIntHolder h) {
    int len1 = h.end - h.start;
    double[] d1 = new double[len1];
    for (int i = h.start; i < h.end; i++) {
      d1[i-h.start] = h.vector.getAccessor().get(i);
    }
    return new double[][] {d1};
  }

  /**
   * Extracting two-dimensions double array from {@link RepeatedBigIntHolder}.
   * @param holder the repeated bigint value holder
   * @return return the two-dimensions double array
   */
  public static double[][] extract2DoubleArray(RepeatedBigIntHolder holder) {
    int len = holder.end - holder.start;
    double[] d = new double[len];
    for (int i = holder.start; i < holder.end; i++) {
      d[i-holder.start] = holder.vector.getAccessor().get(i);
    }
    return new double[][] {d};
  }

  /**
   * Extracting two-dimensions double array from {@link RepeatedSmallIntHolder}.
   * @param holder the repeated smallint value holder
   * @return return the two-dimensions double array
   */
  public static double[][] extract2DoubleArray(RepeatedSmallIntHolder holder) {
    int len = holder.end - holder.start;
    double[] d = new double[len];
    for (int i = holder.start; i < holder.end; i++) {
      d[i-holder.start] = holder.vector.getAccessor().get(i);
    }
    return new double[][] {d};
  }

  /**
   * Extracting two-dimensions double array from {@link RepeatedTinyIntHolder}.
   * @param holder the repeated tinyint value holder
   * @return return the two-dimensions double array
   */
  public static double[][] extract2DoubleArray(RepeatedTinyIntHolder holder) {
    int len = holder.end - holder.start;
    double[] d = new double[len];
    for (int i = holder.start; i < holder.end; i++) {
      d[i-holder.start] = holder.vector.getAccessor().get(i);
    }
    return new double[][] {d};
  }

  /**
   * Extracting two-dimensions double array from {@link RepeatedFloat4Holder}.
   * @param holder the repeated float4 value holder
   * @return return the two-dimensions double array
   */
  public static double[][] extract2DoubleArray(RepeatedFloat4Holder holder) {
    int len = holder.end - holder.start;
    double[] d = new double[len];
    for (int i = holder.start; i < holder.end; i++) {
      d[i-holder.start] = holder.vector.getAccessor().get(i);
    }
    return new double[][] {d};
  }

  /**
   * Extracting two-dimensions double array from {@link RepeatedFloat8Holder}.
   * @param holder the repeated float8 value holder
   * @return return the one-dimensions double array
   */
  public static double[][] extract2DoubleArray(RepeatedFloat8Holder holder) {
    int len = holder.end - holder.start;
    double[] d = new double[len];
    for (int i = holder.start; i < holder.end; i++) {
      d[i-holder.start] = holder.vector.getAccessor().get(i);
    }
    return new double[][] {d};
  }

  /**
   * Extracting one-dimensions double array from {@link RepeatedIntHolder}.
   * @param h the repeated int value holder
   * @return return the one-dimensions double array
   */
  public static double[] extract1DoubleArray(RepeatedIntHolder h) {
    int len1 = h.end - h.start;
    double[] d1 = new double[len1];
    for (int i = h.start; i < h.end; i++) {
      d1[i-h.start] = h.vector.getAccessor().get(i);
    }
    return d1;
  }

  /**
   * Extracting one-dimensions double array from {@link RepeatedBigIntHolder}.
   * @param holder the repeated bigint value holder
   * @return return the one-dimensions double array
   */
  public static double[] extract1DoubleArray(RepeatedBigIntHolder holder) {
    int len = holder.end - holder.start;
    double[] d = new double[len];
    for (int i = holder.start; i < holder.end; i++) {
      d[i-holder.start] = holder.vector.getAccessor().get(i);
    }
    return d;
  }

  /**
   * Extracting one-dimensions double array from {@link RepeatedSmallIntHolder}.
   * @param holder the repeated smallint value holder
   * @return return the one-dimensions double array
   */
  public static double[] extract1DoubleArray(RepeatedSmallIntHolder holder) {
    int len = holder.end - holder.start;
    double[] d = new double[len];
    for (int i = holder.start; i < holder.end; i++) {
      d[i-holder.start] = holder.vector.getAccessor().get(i);
    }
    return d;
  }

  /**
   * Extracting one-dimensions double array from {@link RepeatedTinyIntHolder}.
   * @param holder the repeated tinyint value holder
   * @return return the one-dimensions double array
   */
  public static double[] extract1DoubleArray(RepeatedTinyIntHolder holder) {
    int len = holder.end - holder.start;
    double[] d = new double[len];
    for (int i = holder.start; i < holder.end; i++) {
      d[i-holder.start] = holder.vector.getAccessor().get(i);
    }
    return d;
  }

  /**
   * Extracting one-dimensions double array from {@link RepeatedFloat4Holder}.
   * @param holder the repeated float4 value holder
   * @return return the one-dimensions double array
   */
  public static double[] extract1DoubleArray(RepeatedFloat4Holder holder) {
    int len = holder.end - holder.start;
    double[] d = new double[len];
    for (int i = holder.start; i < holder.end; i++) {
      d[i-holder.start] = holder.vector.getAccessor().get(i);
    }
    return d;
  }

  /**
   * Extracting one-dimensions double array from {@link RepeatedFloat8Holder}.
   * @param holder the repeated float8 value holder
   * @return return the one-dimensions double array
   */
  public static double[] extract1DoubleArray(RepeatedFloat8Holder holder) {
    int len = holder.end - holder.start;
    double[] d = new double[len];
    for (int i = holder.start; i < holder.end; i++) {
      d[i-holder.start] = holder.vector.getAccessor().get(i);
    }
    return d;
  }

  /**
   * Extracting one-dimensions double array from {@link VarBinaryHolder}.
   * @param holder the binary holder
   * @return return the one-dimensions double array
   */
  public static double[] extract1DoubleArray(VarBinaryHolder holder) {
    int length = holder.end - holder.start;
    double[] result = new double[length];
    for (int i = 0; i < length; i++) {
      result[i] = holder.buffer.getDouble(i*8);
    }
    return result;
  }

  /**
   * Set double array to {@link RepeatedFloat8Holder}.
   * @param arr the double array to be set
   * @param out the repeated float8 holder
   */
  public static void setRepeatedFloat8Holder(double[] arr, RepeatedFloat8Holder out) {
    out.vector.allocateNew(arr.length);
    for (int i = 0; i < arr.length; i++) {
      out.vector.getMutator().setSafe(i, arr[i]);
    }
    out.end = arr.length;
  }

  public static void writeList(BaseWriter.ListWriter list, Consumer<BaseWriter.ListWriter> write) {
    list.startList();
    write.accept(list);
    list.endList();
  }

  public static void writeMap(BaseWriter.MapWriter map, Consumer<BaseWriter.MapWriter> write) {
    map.start();
    write.accept(map);
    map.end();
  }

}
