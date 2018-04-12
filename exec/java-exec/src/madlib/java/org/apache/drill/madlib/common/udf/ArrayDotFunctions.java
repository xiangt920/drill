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
package org.apache.drill.madlib.common.udf;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.RepeatedFloat8Holder;
import org.apache.drill.madlib.common.DataHolderUtils;
import org.apache.drill.madlib.jni.DrillJni;

/**
 *
 */

@SuppressWarnings("unused")
public class ArrayDotFunctions {

  public static double arrayDot(double[][] v1, double[][] v2) {
    return DrillJni.JNI.array_dot(v1, v2);
  }

  @FunctionTemplate(name = "array_dot", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class ArrayDotFunction implements DrillSimpleFunc {

    @Param
    RepeatedFloat8Holder in1;
    @Param
    RepeatedFloat8Holder in2;

    @Output
    Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      double[][] v1 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in1);
      double[][] v2 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in2);
      out.value = org.apache.drill.madlib.common.udf.ArrayDotFunctions.arrayDot(v1, v2);
    }
  }
}
