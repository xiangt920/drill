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
package org.apache.drill.madlib.linregr.udf;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.Float8Holder;
import org.apache.drill.exec.expr.holders.RepeatedBigIntHolder;
import org.apache.drill.exec.expr.holders.RepeatedFloat4Holder;
import org.apache.drill.exec.expr.holders.RepeatedFloat8Holder;
import org.apache.drill.exec.expr.holders.RepeatedIntHolder;
import org.apache.drill.exec.expr.holders.RepeatedSmallIntHolder;
import org.apache.drill.exec.expr.holders.RepeatedTinyIntHolder;
import org.apache.drill.madlib.jni.DrillJni;

/**
 * Drill UDFs for linear regression predict.
 *
 */

@SuppressWarnings("unused")
public class LinearPredictFunctions {
  
  public static double predict(double[][] x, double[][] y) {
    return DrillJni.JNI.array_dot(x, y);
  }

  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4TITI implements DrillSimpleFunc {

    @Param
    RepeatedTinyIntHolder in1;
    @Param
    RepeatedTinyIntHolder in2;

    @Output
    Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      double[][] v1 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in1);
      double[][] v2 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in2);
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4TISI implements DrillSimpleFunc {

    @Param
    RepeatedTinyIntHolder in1;
    @Param
    RepeatedSmallIntHolder in2;

    @Output
    Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      double[][] v1 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in1);
      double[][] v2 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in2);
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4TII implements DrillSimpleFunc {

    @Param
    RepeatedTinyIntHolder in1;
    @Param
    RepeatedIntHolder in2;

    @Output
    Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      double[][] v1 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in1);
      double[][] v2 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in2);
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4TIBI implements DrillSimpleFunc {

    @Param
    RepeatedTinyIntHolder in1;
    @Param
    RepeatedBigIntHolder in2;

    @Output
    Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      double[][] v1 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in1);
      double[][] v2 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in2);
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4TIF implements DrillSimpleFunc {

    @Param
    RepeatedTinyIntHolder in1;
    @Param
    RepeatedFloat4Holder in2;

    @Output
    Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      double[][] v1 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in1);
      double[][] v2 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in2);
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4TID implements DrillSimpleFunc {

    @Param
    RepeatedTinyIntHolder in1;
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
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4SITI implements DrillSimpleFunc {

    @Param
    RepeatedSmallIntHolder in1;
    @Param
    RepeatedTinyIntHolder in2;

    @Output
    Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      double[][] v1 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in1);
      double[][] v2 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in2);
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4SISI implements DrillSimpleFunc {

    @Param
    RepeatedSmallIntHolder in1;
    @Param
    RepeatedSmallIntHolder in2;

    @Output
    Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      double[][] v1 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in1);
      double[][] v2 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in2);
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4SII implements DrillSimpleFunc {

    @Param
    RepeatedSmallIntHolder in1;
    @Param
    RepeatedIntHolder in2;

    @Output
    Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      double[][] v1 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in1);
      double[][] v2 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in2);
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4SIBI implements DrillSimpleFunc {

    @Param
    RepeatedSmallIntHolder in1;
    @Param
    RepeatedBigIntHolder in2;

    @Output
    Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      double[][] v1 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in1);
      double[][] v2 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in2);
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4SIF implements DrillSimpleFunc {

    @Param
    RepeatedSmallIntHolder in1;
    @Param
    RepeatedFloat4Holder in2;

    @Output
    Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      double[][] v1 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in1);
      double[][] v2 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in2);
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4SID implements DrillSimpleFunc {

    @Param
    RepeatedSmallIntHolder in1;
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
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4ITI implements DrillSimpleFunc {

    @Param
    RepeatedIntHolder in1;
    @Param
    RepeatedTinyIntHolder in2;

    @Output
    Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      double[][] v1 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in1);
      double[][] v2 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in2);
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4ISI implements DrillSimpleFunc {

    @Param
    RepeatedIntHolder in1;
    @Param
    RepeatedSmallIntHolder in2;

    @Output
    Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      double[][] v1 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in1);
      double[][] v2 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in2);
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4II implements DrillSimpleFunc {

    @Param
    RepeatedIntHolder in1;
    @Param
    RepeatedIntHolder in2;

    @Output
    Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      double[][] v1 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in1);
      double[][] v2 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in2);
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4IBI implements DrillSimpleFunc {

    @Param
    RepeatedIntHolder in1;
    @Param
    RepeatedBigIntHolder in2;

    @Output
    Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      double[][] v1 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in1);
      double[][] v2 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in2);
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4IF implements DrillSimpleFunc {

    @Param
    RepeatedIntHolder in1;
    @Param
    RepeatedFloat4Holder in2;

    @Output
    Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      double[][] v1 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in1);
      double[][] v2 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in2);
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4ID implements DrillSimpleFunc {

    @Param
    RepeatedIntHolder in1;
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
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4BITI implements DrillSimpleFunc {

    @Param
    RepeatedBigIntHolder in1;
    @Param
    RepeatedTinyIntHolder in2;

    @Output
    Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      double[][] v1 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in1);
      double[][] v2 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in2);
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4BISI implements DrillSimpleFunc {

    @Param
    RepeatedBigIntHolder in1;
    @Param
    RepeatedSmallIntHolder in2;

    @Output
    Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      double[][] v1 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in1);
      double[][] v2 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in2);
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4BII implements DrillSimpleFunc {

    @Param
    RepeatedBigIntHolder in1;
    @Param
    RepeatedIntHolder in2;

    @Output
    Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      double[][] v1 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in1);
      double[][] v2 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in2);
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4BIBI implements DrillSimpleFunc {

    @Param
    RepeatedBigIntHolder in1;
    @Param
    RepeatedBigIntHolder in2;

    @Output
    Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      double[][] v1 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in1);
      double[][] v2 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in2);
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4BIF implements DrillSimpleFunc {

    @Param
    RepeatedBigIntHolder in1;
    @Param
    RepeatedFloat4Holder in2;

    @Output
    Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      double[][] v1 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in1);
      double[][] v2 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in2);
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4BID implements DrillSimpleFunc {

    @Param
    RepeatedBigIntHolder in1;
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
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4FTI implements DrillSimpleFunc {

    @Param
    RepeatedFloat4Holder in1;
    @Param
    RepeatedTinyIntHolder in2;

    @Output
    Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      double[][] v1 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in1);
      double[][] v2 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in2);
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4FSI implements DrillSimpleFunc {

    @Param
    RepeatedFloat4Holder in1;
    @Param
    RepeatedSmallIntHolder in2;

    @Output
    Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      double[][] v1 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in1);
      double[][] v2 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in2);
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4FI implements DrillSimpleFunc {

    @Param
    RepeatedFloat4Holder in1;
    @Param
    RepeatedIntHolder in2;

    @Output
    Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      double[][] v1 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in1);
      double[][] v2 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in2);
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4FBI implements DrillSimpleFunc {

    @Param
    RepeatedFloat4Holder in1;
    @Param
    RepeatedBigIntHolder in2;

    @Output
    Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      double[][] v1 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in1);
      double[][] v2 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in2);
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4FF implements DrillSimpleFunc {

    @Param
    RepeatedFloat4Holder in1;
    @Param
    RepeatedFloat4Holder in2;

    @Output
    Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      double[][] v1 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in1);
      double[][] v2 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in2);
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4FD implements DrillSimpleFunc {

    @Param
    RepeatedFloat4Holder in1;
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
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4DTI implements DrillSimpleFunc {

    @Param
    RepeatedFloat8Holder in1;
    @Param
    RepeatedTinyIntHolder in2;

    @Output
    Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      double[][] v1 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in1);
      double[][] v2 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in2);
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4DSI implements DrillSimpleFunc {

    @Param
    RepeatedFloat8Holder in1;
    @Param
    RepeatedSmallIntHolder in2;

    @Output
    Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      double[][] v1 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in1);
      double[][] v2 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in2);
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4DI implements DrillSimpleFunc {

    @Param
    RepeatedFloat8Holder in1;
    @Param
    RepeatedIntHolder in2;

    @Output
    Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      double[][] v1 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in1);
      double[][] v2 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in2);
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4DBI implements DrillSimpleFunc {

    @Param
    RepeatedFloat8Holder in1;
    @Param
    RepeatedBigIntHolder in2;

    @Output
    Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      double[][] v1 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in1);
      double[][] v2 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in2);
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4DF implements DrillSimpleFunc {

    @Param
    RepeatedFloat8Holder in1;
    @Param
    RepeatedFloat4Holder in2;

    @Output
    Float8Holder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      double[][] v1 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in1);
      double[][] v2 = org.apache.drill.madlib.common.DataHolderUtils.extract2DoubleArray(in2);
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
  @FunctionTemplate(name = "linregr_predict", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class Predict4DD implements DrillSimpleFunc {

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
      out.value = org.apache.drill.madlib.linregr.udf.LinearPredictFunctions.predict(v1, v2);
    }
  }
}
