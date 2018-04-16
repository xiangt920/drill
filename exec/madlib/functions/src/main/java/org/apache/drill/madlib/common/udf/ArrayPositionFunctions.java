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

import org.apache.drill.common.Utils;
import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.RepeatedBigIntHolder;
import org.apache.drill.exec.expr.holders.RepeatedFloat8Holder;
import org.apache.drill.exec.expr.holders.RepeatedVarCharHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.vector.complex.writer.BaseWriter;
import org.apache.drill.madlib.common.DataHolderUtils;

import java.util.function.Consumer;

/**
 *
 */

@SuppressWarnings({"unused", "WeakerAccess", "Convert2Lambda"})
public class ArrayPositionFunctions {

  public static void arrayPosition(RepeatedFloat8Holder in, BaseWriter.ComplexWriter writer) {
    final double[] values = DataHolderUtils.extract1DoubleArray(in);
    BaseWriter.ListWriter list = writer.rootAsList();
    DataHolderUtils.writeList(list, new Consumer<BaseWriter.ListWriter>() {
      @Override
      public void accept(BaseWriter.ListWriter l) {
        for (int i = 0; i < values.length; i++) {
          BaseWriter.MapWriter map = l.map();
          final int idx = i;
          DataHolderUtils.writeMap(map, new Consumer<BaseWriter.MapWriter>() {
            @Override
            public void accept(BaseWriter.MapWriter m) {
              m.integer("pos").writeInt(idx + 1);
              m.float8("val").writeFloat8(values[idx]);
            }
          });
        }
      }
    });

  }

  public static void arrayPosition(final RepeatedBigIntHolder in, BaseWriter.ComplexWriter writer) {
    BaseWriter.ListWriter list = writer.rootAsList();
    DataHolderUtils.writeList(list, new Consumer<BaseWriter.ListWriter>() {
      @Override
      public void accept(BaseWriter.ListWriter l) {
        for (int i = in.start; i < in.end; i++) {
          BaseWriter.MapWriter map = l.map();
          final int idx = i;
          DataHolderUtils.writeMap(map, new Consumer<BaseWriter.MapWriter>() {
            @Override
            public void accept(BaseWriter.MapWriter m) {
              m.integer("pos").writeInt(idx - in.start + 1);
              m.bigInt("val").writeBigInt(in.vector.getAccessor().get(idx));
            }
          });
        }
      }
    });

  }

  public static void arrayPosition(RepeatedVarCharHolder in, BaseWriter.ComplexWriter writer) {
    BaseWriter.ListWriter list = writer.rootAsList();
    DataHolderUtils.writeList(list, new Consumer<BaseWriter.ListWriter>() {
      @Override
      public void accept(BaseWriter.ListWriter l) {
        for (int i = in.start; i < in.end; i++) {
          BaseWriter.MapWriter map = l.map();
          final int idx = i;
          DataHolderUtils.writeMap(map, new Consumer<BaseWriter.MapWriter>() {
            @Override
            public void accept(BaseWriter.MapWriter m) {
              m.integer("pos").writeInt(idx - in.start + 1);
              VarCharHolder tmp = new VarCharHolder();
              in.vector.getAccessor().get(idx, tmp);
              m.varChar("val").write(tmp);
            }
          });

        }
      }
    });

  }

  public static void arrayPosition(double[][] valList, BaseWriter.ComplexWriter writer) {
    BaseWriter.ListWriter list = writer.rootAsList();
    DataHolderUtils.writeList(list, new Consumer<BaseWriter.ListWriter>() {
      @Override
      public void accept(BaseWriter.ListWriter l) {
        for (int i = 0; i < valList.length; i++) {
          BaseWriter.MapWriter map = l.map();
          final int idx = i;
          DataHolderUtils.writeMap(map, new Consumer<BaseWriter.MapWriter>() {
            @Override
            public void accept(BaseWriter.MapWriter m) {
              m.integer("pos").writeInt(idx + 1);
              BaseWriter.ListWriter subList = m.list("val");
              DataHolderUtils.writeList(subList, new Consumer<BaseWriter.ListWriter>() {
                @Override
                public void accept(BaseWriter.ListWriter sl) {
                  for (double v : valList[idx]) {
                    sl.float8().writeFloat8(v);
                  }
                }
              });
            }
          });
        }
      }
    });

  }

  public static void arrayPosition(VarCharHolder in, BaseWriter.ComplexWriter writer) {
    String json = DataHolderUtils.extractVarchar(in);
    double[][] valList = Utils.parse_from_json(json, double[][].class);
    arrayPosition(valList, writer);
  }

  public static void arrayPosition(NullableVarCharHolder in, BaseWriter.ComplexWriter writer) {
    if (in.isSet == 0) {
      return;
    }
    String json = DataHolderUtils.extractVarchar(in);
    double[][] valList = Utils.parse_from_json(json, double[][].class);
    arrayPosition(valList, writer);
  }

  @FunctionTemplate(name = "__array_position", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class FloatArrayPosition implements DrillSimpleFunc {

    @Param
    RepeatedFloat8Holder in;

    @Output
    BaseWriter.ComplexWriter writer;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      org.apache.drill.madlib.common.udf.ArrayPositionFunctions.arrayPosition(in, writer);
    }
  }

  @FunctionTemplate(name = "__array_position", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class BigIntArrayPosition implements DrillSimpleFunc {

    @Param
    RepeatedBigIntHolder in;

    @Output
    BaseWriter.ComplexWriter writer;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      org.apache.drill.madlib.common.udf.ArrayPositionFunctions.arrayPosition(in, writer);
    }
  }

  @FunctionTemplate(name = "__array_position", scope = FunctionTemplate.FunctionScope.SIMPLE)
  public static class VarcharArrayPosition implements DrillSimpleFunc {

    @Param
    RepeatedVarCharHolder in;

    @Output
    BaseWriter.ComplexWriter writer;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      org.apache.drill.madlib.common.udf.ArrayPositionFunctions.arrayPosition(in, writer);
    }
  }

  /*
    @param in the parameter must be a two-dimensions double array
   */
  @FunctionTemplate(name = "__array_position",
    scope = FunctionTemplate.FunctionScope.SIMPLE,
    nulls = FunctionTemplate.NullHandling.NULL_IF_NULL
  )
  public static class JsonArrayPosition implements DrillSimpleFunc {

    @Param
    VarCharHolder in;

    @Output
    BaseWriter.ComplexWriter writer;

    @Override
    public void setup() {

    }

    @Override
    public void eval() {
      org.apache.drill.madlib.common.udf.ArrayPositionFunctions.arrayPosition(in, writer);
    }
  }

}
