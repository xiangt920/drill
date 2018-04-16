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

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.BigIntHolder;
import org.apache.drill.exec.expr.holders.RepeatedBigIntHolder;

/**
 * Generate a series of values, from start to stop with a step size of one
 */

@SuppressWarnings("unused")
public class GenerateSeriesFunctions {

  public static void generate(long start, long stop, long step, RepeatedBigIntHolder out) {
    int valueCount = (int) ((stop-start) / step) + 1;
    out.vector.allocateNew(valueCount);
    for (long i = start; i<=stop; i+= step) {
      out.vector.getMutator().setSafe((int) ((i-start) / step), i);
    }
    out.end = valueCount;
  }

  @FunctionTemplate(name = "generate_series", scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class GenerateSeries implements DrillSimpleFunc {

    @Param
    BigIntHolder startHolder;
    @Param BigIntHolder stopHolder;

    @Output
    RepeatedBigIntHolder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      long start = startHolder.value;
      long stop = stopHolder.value;
      org.apache.drill.madlib.common.udf.GenerateSeriesFunctions.generate(start, stop, (long)1, out);
    }
  }

  @FunctionTemplate(name = "generate_series", scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL)
  public static class GenerateSeriesWithStep implements DrillSimpleFunc {

    @Param BigIntHolder startHolder;
    @Param BigIntHolder stopHolder;
    @Param BigIntHolder stepHolder;

    @Output
    RepeatedBigIntHolder out;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      long start = startHolder.value;
      long stop = stopHolder.value;
      long step = stepHolder.value;
      org.apache.drill.madlib.common.udf.GenerateSeriesFunctions.generate(start, stop, step, out);
    }
  }
}
