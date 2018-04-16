/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.drill.madlib.common.udf;

import io.netty.buffer.DrillBuf;
import org.apache.drill.common.Utils;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.RepeatedFloat8Holder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.madlib.common.DataHolderUtils;
import org.apache.drill.madlib.jni.DrillJni;

import javax.inject.Inject;

/**
 * This function takes an array as the input and executes element-wise multiplication
 * by the scalar provided as the second argument, returning the resulting array.
 * It requires that all the values are NON-NULL. Return type is the same as the input type.
 */

@SuppressWarnings("unused")
public class ArrayScalarMultFunctions {

  public static void array_scalar_mult(RepeatedFloat8Holder v1_holder, double v2, RepeatedFloat8Holder out) {
    double[] v1 = DataHolderUtils.extract1DoubleArray(v1_holder);
    double[] result = DrillJni.JNI.array_scalar_mult(v1, v2);
    for (int i = 0; i < result.length; i++) {
      out.vector.getMutator().setSafe(i, result[i]);
    }
    out.end = result.length;
  }

  public static void array_scalar_mult(VarCharHolder v1_holder, double v2, VarCharHolder out, DrillBuf buf) {
    String v1Str = DataHolderUtils.extractVarchar(v1_holder);
    double[][] v1 = Utils.parse_from_json(v1Str, double[][].class);
    for (int i = 0; i < v1.length; i++) {
      for (int j = 0; j < v1[i].length; j++) {
        v1[i][j] *= v2;
      }
    }
    String result = Utils.parse_to_json(v1);
    DataHolderUtils.resetVarcharOut(buf, out, result);
  }

  @FunctionTemplate(name = "array_scalar_mult", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class ArrayScalarMult implements DrillSimpleFunc {

    @Param
    RepeatedFloat8Holder v1_holder;
    @Param
    Float8Holder v2_holder;

    @Output
    RepeatedFloat8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      org.apache.drill.madlib.common.udf.ArrayScalarMultFunctions
        .array_scalar_mult(v1_holder, v2_holder.value, out);

    }
  }

  @FunctionTemplate(name = "array_scalar_mult", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class ArrayScalarMultVarchar implements DrillSimpleFunc {

    @Param
    VarCharHolder v1_holder;
    @Param
    Float8Holder v2_holder;

    @Inject
    DrillBuf buf;

    @Output
    VarCharHolder out;


    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      org.apache.drill.madlib.common.udf.ArrayScalarMultFunctions
        .array_scalar_mult(v1_holder, v2_holder.value, out, buf);
      buf = out.buffer;
    }
  }


}
