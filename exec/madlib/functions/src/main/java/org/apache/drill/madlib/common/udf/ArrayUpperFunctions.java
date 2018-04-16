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
import org.apache.drill.exec.expr.holders.IntHolder;
import org.apache.drill.exec.expr.holders.RepeatedBigIntHolder;
import org.apache.drill.exec.expr.holders.RepeatedFloat4Holder;
import org.apache.drill.exec.expr.holders.RepeatedFloat8Holder;
import org.apache.drill.exec.expr.holders.RepeatedIntHolder;
import org.apache.drill.exec.expr.holders.RepeatedSmallIntHolder;
import org.apache.drill.exec.expr.holders.RepeatedTinyIntHolder;

/**
 */

@SuppressWarnings("unused")
public class ArrayUpperFunctions {
  @FunctionTemplate(name = "array_upper", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class ArrayUpperFunction4TI implements DrillSimpleFunc {

    @Param
    RepeatedTinyIntHolder in1;

    @Output
    IntHolder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      out.value = in1.end - in1.start - 1;
    }
  }
  @FunctionTemplate(name = "array_upper", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class ArrayUpperFunction4SI implements DrillSimpleFunc {

    @Param
    RepeatedSmallIntHolder in1;

    @Output
    IntHolder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      out.value = in1.end - in1.start - 1;
    }
  }
  @FunctionTemplate(name = "array_upper", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class ArrayUpperFunction4I implements DrillSimpleFunc {

    @Param
    RepeatedIntHolder in1;

    @Output
    IntHolder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      out.value = in1.end - in1.start - 1;
    }
  }
  @FunctionTemplate(name = "array_upper", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class ArrayUpperFunction4BI implements DrillSimpleFunc {

    @Param
    RepeatedBigIntHolder in1;

    @Output
    IntHolder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      out.value = in1.end - in1.start - 1;
    }
  }
  @FunctionTemplate(name = "array_upper", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class ArrayUpperFunction4F implements DrillSimpleFunc {

    @Param
    RepeatedFloat4Holder in1;

    @Output
    IntHolder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      out.value = in1.end - in1.start - 1;
    }
  }
  @FunctionTemplate(name = "array_upper", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class ArrayUpperFunction4D implements DrillSimpleFunc {

    @Param
    RepeatedFloat8Holder in1;

    @Output
    IntHolder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      out.value = in1.end - in1.start - 1;
    }
  }
}
