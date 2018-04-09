/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.drill.exec.ops;

import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableSet;
import com.sun.codemodel.JAssignmentTarget;
import com.sun.codemodel.JBlock;
import com.sun.codemodel.JCodeModel;
import com.sun.codemodel.JConditional;
import com.sun.codemodel.JExpr;
import com.sun.codemodel.JFieldRef;
import com.sun.codemodel.JType;
import com.sun.codemodel.JVar;
import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.apache.drill.exec.expr.ClassGenerator;
import org.apache.drill.exec.expr.holders.Decimal18Holder;
import org.apache.drill.exec.expr.holders.Decimal28SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal38SparseHolder;
import org.apache.drill.exec.expr.holders.Decimal9Holder;
import org.apache.drill.exec.expr.holders.IntervalDayHolder;
import org.apache.drill.exec.expr.holders.IntervalYearHolder;
import org.apache.drill.exec.expr.holders.NullableDecimal18Holder;
import org.apache.drill.exec.expr.holders.NullableDecimal28SparseHolder;
import org.apache.drill.exec.expr.holders.NullableDecimal38SparseHolder;
import org.apache.drill.exec.expr.holders.NullableDecimal9Holder;
import org.apache.drill.exec.expr.holders.NullableIntervalDayHolder;
import org.apache.drill.exec.expr.holders.NullableIntervalYearHolder;
import org.apache.drill.exec.expr.holders.NullableVarCharHolder;
import org.apache.drill.exec.expr.holders.RepeatedBigIntHolder;
import org.apache.drill.exec.expr.holders.RepeatedBitHolder;
import org.apache.drill.exec.expr.holders.RepeatedDecimal18Holder;
import org.apache.drill.exec.expr.holders.RepeatedDecimal28SparseHolder;
import org.apache.drill.exec.expr.holders.RepeatedDecimal38SparseHolder;
import org.apache.drill.exec.expr.holders.RepeatedDecimal9Holder;
import org.apache.drill.exec.expr.holders.RepeatedFloat4Holder;
import org.apache.drill.exec.expr.holders.RepeatedFloat8Holder;
import org.apache.drill.exec.expr.holders.RepeatedIntHolder;
import org.apache.drill.exec.expr.holders.RepeatedIntervalDayHolder;
import org.apache.drill.exec.expr.holders.RepeatedIntervalYearHolder;
import org.apache.drill.exec.expr.holders.RepeatedSmallIntHolder;
import org.apache.drill.exec.expr.holders.RepeatedTinyIntHolder;
import org.apache.drill.exec.expr.holders.RepeatedVar16CharHolder;
import org.apache.drill.exec.expr.holders.RepeatedVarBinaryHolder;
import org.apache.drill.exec.expr.holders.RepeatedVarCharHolder;
import org.apache.drill.exec.expr.holders.ValueHolder;
import org.apache.drill.exec.expr.holders.VarCharHolder;
import org.apache.drill.exec.memory.BufferAllocator;
import org.apache.drill.exec.record.MaterializedField;
import org.apache.drill.exec.vector.BaseValueVector;
import org.apache.drill.exec.vector.BigIntVector;
import org.apache.drill.exec.vector.BitVector;
import org.apache.drill.exec.vector.Decimal18Vector;
import org.apache.drill.exec.vector.Decimal28SparseVector;
import org.apache.drill.exec.vector.Decimal38SparseVector;
import org.apache.drill.exec.vector.Decimal9Vector;
import org.apache.drill.exec.vector.Float4Vector;
import org.apache.drill.exec.vector.Float8Vector;
import org.apache.drill.exec.vector.IntVector;
import org.apache.drill.exec.vector.IntervalDayVector;
import org.apache.drill.exec.vector.IntervalYearVector;
import org.apache.drill.exec.vector.ValueHolderHelper;
import org.apache.drill.exec.vector.VarCharVector;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static org.apache.drill.common.expression.visitors.ArrayConstVisitor.INSTANCE;

/**
 * Enum for array elements' type.
 */

public enum ArrayMinorType {
  /**
   *
   * <pre>
   *  variable length binary
   * </pre>
   * @see TypeProtos.MinorType#VARCHAR
   */
  VARCHAR {

    @Override
    public ValueHolder newValueHolder(BufferManager manager, BufferAllocator allocator, List<LogicalExpression> args) {
      RepeatedVarCharHolder holder = new RepeatedVarCharHolder();
      holder.vector = new VarCharVector(MaterializedField.create("_array", Types.required(TypeProtos.MinorType.VARCHAR)), allocator);
      int size = args.isEmpty()?1:args.size();
      holder.vector.allocateNew(size*Types.MAX_VARCHAR_LENGTH, size);
      addValues(holder, args);
      if (manager != null) {
        manager.manageBuffer(holder.vector.getBuffer());
        manager.manageBuffer(holder.vector.getOffsetVector().getBuffer());
      }
      return holder;
    }

    @Override
    public ValueHolder addValueSafe(LogicalExpression e, ValueHolder holder, int index) {
      RepeatedVarCharHolder h = ((RepeatedVarCharHolder) holder);
      addValueSafe(e, h.vector, index);
      h.end++;
      return holder;
    }

    @Override
    public BaseValueVector addValueSafe(Object val, BaseValueVector vector, int index) {
      VarCharVector h = ((VarCharVector) vector);
      if (val == null) {
        h.getMutator().setSafe(index, "".getBytes(Charsets.UTF_8));
      } else if (val instanceof LogicalExpression) {
        LogicalExpression e = (LogicalExpression) val;

        h.getMutator().setSafe(index, (e.accept(INSTANCE, null)).toString().getBytes(Charsets.UTF_8));

      } else if (val instanceof VarCharHolder) {
        h.getMutator().setSafe(index, (VarCharHolder) val);
      } else if (val instanceof NullableVarCharHolder) {
        NullableVarCharHolder valHolder = (NullableVarCharHolder) val;
        if (valHolder.isSet == 0) {
          h.getMutator().setSafe(index, "".getBytes(Charsets.UTF_8));
        } else {
          h.getMutator().setSafe(index, (NullableVarCharHolder) val);
        }
      } else if(val instanceof String) {
        h.getMutator().setSafe(index, ((String) val).getBytes(Charsets.UTF_8));
      } else {
        h.getMutator().setSafe(index, val.toString().getBytes(Charsets.UTF_8));
      }
      return vector;
    }

    @Override
    public BaseValueVector getVector(int size, MaterializedField field, BufferAllocator allocator) {
      VarCharVector  v = new VarCharVector (field, allocator);
      v.allocateNew(size* Types.MAX_VARCHAR_LENGTH, size);
      return v;
    }

  },

  /**
   * <pre>
   *  four byte signed integer
   * </pre>
   * @see TypeProtos.MinorType#INT
   */
  INTEGER {

    @Override
    public ValueHolder newValueHolder(BufferManager manager, BufferAllocator allocator, List<LogicalExpression> args) {
      RepeatedIntHolder holder = new RepeatedIntHolder();
      holder.vector = new IntVector(MaterializedField.create("_array", Types.required(TypeProtos.MinorType.INT)), allocator);
      holder.vector.allocateNew(args.isEmpty()?1:args.size());
      addValues(holder, args);
      if (manager != null) {
        manager.manageBuffer(holder.vector.getBuffer());
      }
      return holder;
    }

    @Override
    public ValueHolder addValueSafe(LogicalExpression e, ValueHolder holder, int index) {
      RepeatedIntHolder h = ((RepeatedIntHolder) holder);
      addValueSafe(e, h.vector, index);
      h.end++;

      return holder;
    }

    @Override
    public BaseValueVector addValueSafe(Object val, BaseValueVector vector, int index) {
      IntVector h = ((IntVector) vector);
      if (val == null) {
        h.getMutator().setSafe(index, 0);
      } else if (val instanceof LogicalExpression) {
        LogicalExpression e = (LogicalExpression) val;
        h.getMutator().setSafe(index, ((Number) e.accept(INSTANCE, null)).intValue());
      } else if(val instanceof Number) {
        h.getMutator().setSafe(index, ((Number) val).intValue());
      } else if (val instanceof String) {
        h.getMutator().setSafe(index, Integer.valueOf((String) val));
      } else if(val instanceof Boolean) {
        h.getMutator().setSafe(index, ((Boolean) val)?1:0);
      } else {
        throw new IllegalArgumentException("Unsupported type for integer type while the value is:" + val.getClass().getCanonicalName());
      }

      return vector;
    }

    @Override
    public BaseValueVector getVector(int size, MaterializedField field, BufferAllocator allocator) {
      IntVector  v = new IntVector (field, allocator);
      v.allocateNew(size);
      return v;
    }

  },

  /**
   * <pre>
   *  eight byte signed integer
   * </pre>
   * @see TypeProtos.MinorType#BIGINT
   */
  BIGINT {

    @Override
    public ValueHolder newValueHolder(BufferManager manager, BufferAllocator allocator, List<LogicalExpression> args) {
      RepeatedBigIntHolder holder = new RepeatedBigIntHolder();
      holder.vector = new BigIntVector(MaterializedField.create("_array", Types.required(TypeProtos.MinorType.BIGINT)), allocator);
      holder.vector.allocateNew(args.isEmpty()?1:args.size());
      addValues(holder, args);
      if (manager != null) {
        manager.manageBuffer(holder.vector.getBuffer());
      }
      return holder;
    }

    @Override
    public ValueHolder addValueSafe(LogicalExpression e, ValueHolder holder, int index) {
      RepeatedBigIntHolder h = ((RepeatedBigIntHolder) holder);
      addValueSafe(e, h.vector, index);
      h.end++;

      return holder;
    }

    @Override
    public BaseValueVector addValueSafe(Object val, BaseValueVector vector, int index) {
      BigIntVector h = ((BigIntVector) vector);
      if (val == null) {
        h.getMutator().setSafe(index, 0);
      }else if (val instanceof LogicalExpression) {
        LogicalExpression e = (LogicalExpression) val;
        h.getMutator().setSafe(index, ((Number) e.accept(INSTANCE, null)).longValue());
      } else if (val instanceof Number) {
        h.getMutator().setSafe(index, ((Number) val).longValue());
      } else if (val instanceof String) {
        h.getMutator().setSafe(index, Long.valueOf((String) val));
      } else if(val instanceof Boolean) {
        h.getMutator().setSafe(index, ((Boolean) val)?1:0);
      } else {
        throw new IllegalArgumentException("Unsupported type for bigint type while the value is:" + val.getClass().getCanonicalName());
      }

      return vector;
    }

    @Override
    public BaseValueVector getVector(int size, MaterializedField field, BufferAllocator allocator) {
      BigIntVector  v = new BigIntVector (field, allocator);
      v.allocateNew(size);
      return v;
    }

  },

  /**
   * <pre>
   *  4 byte ieee 754
   * </pre>
   * @see TypeProtos.MinorType#FLOAT4
   */
  FLOAT {

    @Override
    public ValueHolder newValueHolder(BufferManager manager, BufferAllocator allocator, List<LogicalExpression> args) {
      RepeatedFloat4Holder holder = new RepeatedFloat4Holder();
      holder.vector = new Float4Vector(MaterializedField.create("_array", Types.required(TypeProtos.MinorType.FLOAT4)), allocator);
      holder.vector.allocateNew(args.isEmpty()?1:args.size());
      addValues(holder, args);
      if (manager != null) {
        manager.manageBuffer(holder.vector.getBuffer());
      }
      return holder;
    }

    @Override
    public ValueHolder addValueSafe(LogicalExpression e, ValueHolder holder, int index) {
      RepeatedFloat4Holder h = ((RepeatedFloat4Holder) holder);
      addValueSafe(e, h.vector, index);
      h.end++;
      return holder;
    }

    @Override
    public BaseValueVector addValueSafe(Object e, BaseValueVector vector, int index) {

      Float4Vector h = ((Float4Vector) vector);
      if (e == null) {
        h.getMutator().setSafe(index, 0);
      } else if (e instanceof LogicalExpression) {
        h.getMutator().setSafe(index, ((Number) ((LogicalExpression) e).accept(INSTANCE, null)).floatValue());
      } else if(e instanceof Number) {
        h.getMutator().setSafe(index, ((Number) e).floatValue());
      } else if (e instanceof String) {
        h.getMutator().setSafe(index, Float.valueOf((String) e));
      } else if(e instanceof Boolean) {
        h.getMutator().setSafe(index, ((Boolean) e)?1:0);
      } else {
        throw new IllegalArgumentException("Unsupported type for float type while the value is:" + e.getClass().getName());
      }
      return vector;
    }

    @Override
    public BaseValueVector getVector(int size, MaterializedField field, BufferAllocator allocator) {
      Float4Vector  v = new Float4Vector (field, allocator);
      v.allocateNew(size);
      return v;
    }

  },

  /**
   * <pre>
   *  8 byte ieee 754
   * </pre>
   * @see TypeProtos.MinorType#FLOAT8
   */
  DOUBLE {

    @Override
    public ValueHolder newValueHolder(BufferManager manager, BufferAllocator allocator, List<LogicalExpression> args) {
      RepeatedFloat8Holder holder = new RepeatedFloat8Holder();
      holder.vector = new Float8Vector(MaterializedField.create("_array", Types.required(TypeProtos.MinorType.FLOAT8)), allocator);
      holder.vector.allocateNew(args.isEmpty()?1:args.size());
      addValues(holder, args);
      if (manager != null) {
        manager.manageBuffer(holder.vector.getBuffer());
      }
      return holder;
    }

    @Override
    public ValueHolder addValueSafe(LogicalExpression e, ValueHolder holder, int index) {
      RepeatedFloat8Holder h = ((RepeatedFloat8Holder) holder);
      addValueSafe(e, h.vector, index);
      h.end++;
      return holder;
    }

    @Override
    public BaseValueVector addValueSafe(Object e, BaseValueVector vector, int index) {
      Float8Vector h = ((Float8Vector) vector);
      if (e == null) {
        h.getMutator().setSafe(index, 0);
      } else if (e instanceof LogicalExpression) {
        h.getMutator().setSafe(index, ((Number) ((LogicalExpression) e).accept(INSTANCE, null)).doubleValue());
      } else if(e instanceof Number) {
        h.getMutator().setSafe(index, ((Number) e).doubleValue());
      } else if (e instanceof String) {
        h.getMutator().setSafe(index, Double.valueOf((String) e));
      } else if(e instanceof Boolean) {
        h.getMutator().setSafe(index, ((Boolean) e)?1:0);
      } else {
        throw new IllegalArgumentException("Unsupported type for double type while the value is:" + e.getClass().getName());
      }
      return vector;
    }

    @Override
    public BaseValueVector getVector(int size, MaterializedField field, BufferAllocator allocator) {
      Float8Vector  v = new Float8Vector (field, allocator);
      v.allocateNew(size);
      return v;
    }

  },

  /**
   * <pre>
   *  a decimal supporting precision between 1 and 9
   * </pre>
   * @see TypeProtos.MinorType#DECIMAL9
   */
  DECIMAL9 {

    @Override
    public ValueHolder newValueHolder(BufferManager manager, BufferAllocator allocator, List<LogicalExpression> args) {
      RepeatedDecimal9Holder holder = new RepeatedDecimal9Holder();
      holder.vector = new Decimal9Vector(MaterializedField.create("_array", Types.required(TypeProtos.MinorType.DECIMAL9)), allocator);
      holder.vector.allocateNew(args.isEmpty()?1:args.size());
      addValues(holder, args);
      if (manager != null) {
        manager.manageBuffer(holder.vector.getBuffer());
      }
      return holder;
    }

    @Override
    public ValueHolder addValueSafe(LogicalExpression e, ValueHolder holder, int index) {
      RepeatedDecimal9Holder h = ((RepeatedDecimal9Holder) holder);
      addValueSafe(e, h.vector, index);
      h.end++;
      return holder;
    }

    @Override
    public BaseValueVector addValueSafe(Object e, BaseValueVector vector, int index) {
      Decimal9Vector h = ((Decimal9Vector) vector);
      if (e instanceof ValueExpressions.Decimal9Expression) {
        h.getMutator().setSafe(index, (Integer) ((ValueExpressions.Decimal9Expression) e).accept(INSTANCE, null));
      } else if (e instanceof Decimal9Holder) {
        h.getMutator().setSafe(index, (Decimal9Holder) e);
      } else if (e instanceof NullableDecimal9Holder) {
        NullableDecimal9Holder valHolder = (NullableDecimal9Holder) e;
        if (valHolder.isSet != 0) {
          h.getMutator().setSafe(index, (NullableDecimal9Holder) e);
        }
      } else if(e instanceof Integer) {
        h.getMutator().setSafe(index, (Integer) e);
      } else {
        throw new IllegalArgumentException("Unsupported type for decimal type while the value is:" + e.getClass().getName());
      }
      return vector;
    }

    @Override
    public BaseValueVector getVector(int size, MaterializedField field, BufferAllocator allocator) {
      Decimal9Vector  v = new Decimal9Vector (field, allocator);
      v.allocateNew(size);
      return v;
    }

  },

  /**
   * <pre>
   *  a decimal supporting precision between 10 and 18
   * </pre>
   * @see TypeProtos.MinorType#DECIMAL18
   */
  DECIMAL18 {

    @Override
    public ValueHolder newValueHolder(BufferManager manager, BufferAllocator allocator, List<LogicalExpression> args) {
      RepeatedDecimal18Holder holder = new RepeatedDecimal18Holder();
      holder.vector = new Decimal18Vector(MaterializedField.create("_array", Types.required(TypeProtos.MinorType.DECIMAL18)), allocator);
      holder.vector.allocateNew(args.isEmpty()?1:args.size());
      addValues(holder, args);
      if (manager != null) {
        manager.manageBuffer(holder.vector.getBuffer());
      }
      return holder;
    }

    @Override
    public ValueHolder addValueSafe(LogicalExpression e, ValueHolder holder, int index) {
      RepeatedDecimal18Holder h = ((RepeatedDecimal18Holder) holder);
      addValueSafe(e, h.vector, index);
      h.end++;
      return holder;
    }

    @Override
    public BaseValueVector addValueSafe(Object e, BaseValueVector vector, int index) {
      Decimal18Vector v = ((Decimal18Vector) vector);
      if (e instanceof ValueExpressions.Decimal18Expression) {
        v.getMutator().setSafe(index, (Long) ((ValueExpressions.Decimal18Expression) e).accept(INSTANCE, null));
      } else if (e instanceof Decimal18Holder) {
        v.getMutator().setSafe(index, (Decimal18Holder) e);
      } else if (e instanceof NullableDecimal18Holder) {
        NullableDecimal18Holder valHolder = (NullableDecimal18Holder) e;
        if (valHolder.isSet != 0) {
          v.getMutator().setSafe(index, (NullableDecimal18Holder) e);
        }
      } else if(e instanceof Long) {
        v.getMutator().setSafe(index, (Long) e);
      } else {
        throw new IllegalArgumentException("Unsupported type for decimal type while the value is:" + e.getClass().getName());
      }
      return vector;
    }

    @Override
    public BaseValueVector getVector(int size, MaterializedField field, BufferAllocator allocator) {
      Decimal18Vector v = new Decimal18Vector(field, allocator);
      v.allocateNew(size);
      return v;
    }

  },

  /**
   * <pre>
   *  a decimal supporting precision between 19 and 28
   * </pre>
   * @see TypeProtos.MinorType#DECIMAL28SPARSE
   */
  DECIMAL28SPARSE {
    @Override
    public ValueHolder newValueHolder(BufferManager manager, BufferAllocator allocator, List<LogicalExpression> args) {
      RepeatedDecimal28SparseHolder holder = new RepeatedDecimal28SparseHolder();
      holder.vector = new Decimal28SparseVector(MaterializedField.create("_array", Types.required(TypeProtos.MinorType.DECIMAL28SPARSE)), allocator);
      holder.vector.allocateNew(args.isEmpty()?1:args.size());
      addValues(holder, args);
      if (manager != null) {
        manager.manageBuffer(holder.vector.getBuffer());
      }
      return holder;
    }

    @Override
    public ValueHolder addValueSafe(LogicalExpression e, ValueHolder holder, int index) {
      RepeatedDecimal28SparseHolder d28Holder = ((RepeatedDecimal28SparseHolder) holder);
      addValueSafe(e, d28Holder.vector, index);
      d28Holder.end++;
      return holder;
    }

    @Override
    public BaseValueVector addValueSafe(Object e, BaseValueVector vector, int index) {
      Decimal28SparseVector v = ((Decimal28SparseVector) vector);
      if (e instanceof ValueExpressions.Decimal28Expression) {
        Decimal28SparseHolder valHolder = ValueHolderHelper.getDecimal28Holder(
          v.getAllocator().getEmpty(),
          (String) ((ValueExpressions.Decimal28Expression) e).accept(INSTANCE, null));
        v.getMutator().setSafe(index, valHolder);
      } else if (e instanceof Decimal28SparseHolder) {
        v.getMutator().setSafe(index, (Decimal28SparseHolder) e);
      } else if (e instanceof NullableDecimal28SparseHolder) {
        NullableDecimal28SparseHolder valHolder = (NullableDecimal28SparseHolder) e;
        if (valHolder.isSet != 0) {
          v.getMutator().setSafe(index, (NullableDecimal28SparseHolder) e);
        }
      } else if(e instanceof String) {
        Decimal28SparseHolder valHolder = ValueHolderHelper.getDecimal28Holder(v.getAllocator().getEmpty(), (String) e);
        v.getMutator().setSafe(index, valHolder);
      } else {
        throw new IllegalArgumentException("Unsupported type for decimal type while the value is:" + e.getClass().getName());
      }
      return vector;
    }

    @Override
    public BaseValueVector getVector(int size, MaterializedField field, BufferAllocator allocator) {
      Decimal28SparseVector v = new Decimal28SparseVector(field, allocator);
      v.allocateNew(size);
      return v;
    }

  },

  /**
   * <pre>
   *  a decimal supporting precision between 29 and 38
   * </pre>
   * @see TypeProtos.MinorType#DECIMAL38SPARSE
   */
  DECIMAL38SPARSE {

    @Override
    public ValueHolder newValueHolder(BufferManager manager, BufferAllocator allocator, List<LogicalExpression> args) {
      RepeatedDecimal38SparseHolder holder = new RepeatedDecimal38SparseHolder();
      holder.vector = new Decimal38SparseVector(MaterializedField.create("_array", Types.required(TypeProtos.MinorType.DECIMAL38SPARSE)), allocator);
      holder.vector.allocateNew(args.isEmpty()?1:args.size());
      addValues(holder, args);
      if (manager != null) {
        manager.manageBuffer(holder.vector.getBuffer());
      }
      return holder;
    }

    @Override
    public ValueHolder addValueSafe(LogicalExpression e, ValueHolder holder, int index) {
      RepeatedDecimal38SparseHolder d38Holder = ((RepeatedDecimal38SparseHolder) holder);
      addValueSafe(e, d38Holder.vector, index);
      d38Holder.end++;
      return holder;
    }

    @Override
    public BaseValueVector addValueSafe(Object e, BaseValueVector vector, int index) {
      Decimal38SparseVector v = ((Decimal38SparseVector) vector);
      if (e instanceof ValueExpressions.Decimal38Expression) {
        Decimal38SparseHolder valHolder = ValueHolderHelper.getDecimal38Holder(
          v.getAllocator().getEmpty(),
          (String) ((ValueExpressions.Decimal38Expression) e).accept(INSTANCE, null));
        v.getMutator().setSafe(index, valHolder);
      } else if (e instanceof Decimal38SparseHolder) {
        v.getMutator().setSafe(index, (Decimal38SparseHolder) e);
      } else if (e instanceof NullableDecimal38SparseHolder) {
        NullableDecimal38SparseHolder valHolder = (NullableDecimal38SparseHolder) e;
        if (valHolder.isSet != 0) {
          v.getMutator().setSafe(index, (NullableDecimal38SparseHolder) e);
        }
      } else if(e instanceof String) {
        Decimal38SparseHolder valHolder = ValueHolderHelper.getDecimal38Holder(v.getAllocator().getEmpty(), (String) e);
        v.getMutator().setSafe(index, valHolder);
      } else {
        throw new IllegalArgumentException("Unsupported type for decimal type while the value is:" + e.getClass().getName());
      }
      return vector;
    }

    @Override
    public BaseValueVector getVector(int size, MaterializedField field, BufferAllocator allocator) {
      Decimal38SparseVector v = new Decimal38SparseVector(field, allocator);
      v.allocateNew(size);
      return v;
    }

  },

  /**
   * <pre>
   * Interval type specifying YEAR to MONTH
   * </pre>
   * @see TypeProtos.MinorType#INTERVALYEAR
   */
  INTERVAL_YEAR_MONTH {

    @Override
    public ValueHolder newValueHolder(BufferManager manager, BufferAllocator allocator, List<LogicalExpression> args) {
      RepeatedIntervalYearHolder holder = new RepeatedIntervalYearHolder();
      holder.vector = new IntervalYearVector(MaterializedField.create("_array", Types.required(TypeProtos.MinorType.INTERVALYEAR)), allocator);
      holder.vector.allocateNew(args.isEmpty()?1:args.size());
      addValues(holder, args);
      if (manager != null) {
        manager.manageBuffer(holder.vector.getBuffer());
      }
      return holder;
    }

    @Override
    public ValueHolder addValueSafe(LogicalExpression e, ValueHolder holder, int index) {
      RepeatedIntervalYearHolder h = ((RepeatedIntervalYearHolder) holder);
      addValueSafe(e, h.vector, index);
      h.end++;
      return holder;
    }

    @Override
    public BaseValueVector addValueSafe(Object e, BaseValueVector vector, int index) {
      IntervalYearVector v = (IntervalYearVector) vector;
      if (e instanceof ValueExpressions.IntervalYearExpression) {
        int val = (Integer) ((ValueExpressions.IntervalYearExpression) e).accept(INSTANCE, null);
        v.getMutator().setSafe(index, val);
      } else if (e instanceof IntervalYearHolder) {
        v.getMutator().setSafe(index, (IntervalYearHolder) e);
      } else if (e instanceof NullableIntervalYearHolder) {
        NullableIntervalYearHolder valHolder = (NullableIntervalYearHolder) e;
        if (valHolder.isSet != 0) {
          v.getMutator().setSafe(index, (NullableIntervalYearHolder) e);
        }
      } else if(e instanceof Integer) {
        v.getMutator().setSafe(index, (Integer) e);
      } else {
        throw new IllegalArgumentException("Unsupported type for interval type while the value is:" + e.getClass().getName());
      }
      return vector;
    }

    @Override
    public BaseValueVector getVector(int size, MaterializedField field, BufferAllocator allocator) {
      IntervalYearVector v = new IntervalYearVector(field, allocator);
      v.allocateNew(size);
      return v;

    }

  },

  /**
   * <pre>
   * Interval type specifying DAY to SECONDS
   * </pre>
   * @see TypeProtos.MinorType#INTERVALDAY
   */
  INTERVAL_DAY_TIME {

    @Override
    public ValueHolder newValueHolder(BufferManager manager, BufferAllocator allocator, List<LogicalExpression> args) {
      RepeatedIntervalDayHolder holder = new RepeatedIntervalDayHolder();
      holder.vector = new IntervalDayVector(MaterializedField.create("_array", Types.required(TypeProtos.MinorType.INTERVALDAY)), allocator);
      holder.vector.allocateNew(args.isEmpty()?1:args.size());
      addValues(holder, args);
      if (manager != null) {
        manager.manageBuffer(holder.vector.getBuffer());
      }
      return holder;
    }

    @Override
    public ValueHolder addValueSafe(LogicalExpression e, ValueHolder holder, int index) {
      RepeatedIntervalDayHolder h = ((RepeatedIntervalDayHolder) holder);
      addValueSafe(e, h.vector, index);
      h.end++;
      return holder;
    }

    @Override
    public BaseValueVector addValueSafe(Object e, BaseValueVector vector, int index) {
      IntervalDayVector v = ((IntervalDayVector) vector);
      if (e instanceof ValueExpressions.IntervalDayExpression) {
        int[] val = (int[]) ((ValueExpressions.IntervalDayExpression) e).accept(INSTANCE, null);
        v.getMutator().setSafe(index, val[0], val[1]);
      } else if (e instanceof IntervalDayHolder) {
        v.getMutator().setSafe(index, (IntervalDayHolder) e);
      } else if (e instanceof NullableIntervalDayHolder) {
        NullableIntervalDayHolder valHolder = (NullableIntervalDayHolder) e;
        if (valHolder.isSet != 0) {
          v.getMutator().setSafe(index, (NullableIntervalDayHolder) e);
        }
      } else if(e instanceof int[]) {
        int[] val = (int[]) e;
        v.getMutator().setSafe(index, val[0], val[1]);
      } else {
        throw new IllegalArgumentException("Unsupported type for interval type while the value is:" + e.getClass().getName());
      }
      return vector;
    }

    @Override
    public BaseValueVector getVector(int size, MaterializedField field, BufferAllocator allocator) {
      IntervalDayVector v = new IntervalDayVector(field, allocator);
      v.allocateNew(size);
      return v;
    }

  },

  /**
   * <pre>
   *  single bit value (boolean)
   * </pre>
   * @see TypeProtos.MinorType#BIT
   */
  BOOLEAN {

    @Override
    public ValueHolder newValueHolder(BufferManager manager, BufferAllocator allocator, List<LogicalExpression> args) {
      RepeatedBitHolder holder = new RepeatedBitHolder();
      holder.vector = new BitVector(MaterializedField.create("_array", Types.required(TypeProtos.MinorType.BIT)), allocator);
      holder.vector.allocateNew(args.isEmpty()?1:args.size());
      addValues(holder, args);
      if (manager != null) {
        manager.manageBuffer(holder.vector.getBuffer());
      }
      return holder;
    }

    @Override
    public ValueHolder addValueSafe(LogicalExpression e, ValueHolder holder, int index) {
      RepeatedBitHolder h = ((RepeatedBitHolder) holder);
      addValueSafe(e, h.vector, index);
      h.end++;
      return holder;
    }

    @Override
    public BaseValueVector addValueSafe(Object e, BaseValueVector vector, int index) {
      if (e instanceof ValueExpressions.BooleanExpression) {
        BitVector v = ((BitVector) vector);
        v.getMutator().setSafe(index, (Boolean) ((ValueExpressions.BooleanExpression) e).accept(INSTANCE, null) ? 1 : 0);
      } else if(e instanceof Boolean) {
        BitVector  v = (BitVector ) vector;
        v.getMutator().setSafe(index, ((Boolean) e)?1:0);
      } else {
        throw new IllegalArgumentException("Unsupported type for boolean type while the value is:" + e.getClass().getName());
      }
      return vector;
    }

    @Override
    public BaseValueVector getVector(int size, MaterializedField field, BufferAllocator allocator) {
      BitVector v = new BitVector(field, allocator);
      v.allocateNew(size);
      return v;
    }

  }, BINARY {
    @Override
    public ValueHolder newValueHolder(BufferManager manager, BufferAllocator allocator, List<LogicalExpression> args) {
      throw new UnsupportedOperationException("Unsupported getting value holder for binary data");
    }

    @Override
    public BaseValueVector getVector(int size, MaterializedField field, BufferAllocator allocator) {
      throw new UnsupportedOperationException("Unsupported getting vector for binary data");
    }
  };

  /**
   * Get a new {@link ValueHolder} and set values.
   * We must re-manage the buffer of the value holder unless we can ensure that
   * the initial {@code args.size()} greater than or equal the actual size of the value holder.
   * If we can't, we must set the {@code manager} to {@code null} and manage the buffer of the
   * value holder after assigning the values.
   * @param manager a buffer manager to manage the buffer of the value holder
   * @param allocator an allocator to allocate buffer for the value holder
   * @param args initial values of the value holder
   * @return - the value holder
   * 
   */
  public ValueHolder newValueHolder(BufferManager manager, BufferAllocator allocator, List<LogicalExpression> args) {
    return null;
  }

  protected void addValues(ValueHolder holder, List<LogicalExpression> values) {
    Iterator<LogicalExpression> it = values.iterator();
    int idx = 0;
    while (it.hasNext()) {
      addValueSafe(it.next(), holder, idx++);
    }
  }

  /**
   * Add value into a {@link BaseValueVector} safely.
   * @param v reference of the value
   * @param vector the value vector
   * @param index position of the value in the value vector
   * @return - the value vector
   */
  @SuppressWarnings({"unused", "UnusedReturnValue"})
  public BaseValueVector addValueSafe(Object v, BaseValueVector vector, int index) {
    return vector;
  }

  /**
   * Add value into a {@link ValueHolder} safely.
   * @param e expression of the value
   * @param holder the value holder
   * @param index position of the value in the value holder
   * @return - the value holder
   */
  @SuppressWarnings("UnusedReturnValue")
  public ValueHolder addValueSafe(LogicalExpression e, ValueHolder holder, int index) {
    return holder;
  }

  /**
   * Get a new {@link BaseValueVector}.
   * @param size value count in the value vector
   * @param field field information for this value vector
   * @param allocator a drill buffer allocator
   * @return - the new {@link BaseValueVector}
   */
  @SuppressWarnings("unused")
  public BaseValueVector getVector(int size, MaterializedField field, BufferAllocator allocator) {
    return null;
  }

  protected JAssignmentTarget assignJValue(JVar holder, TypeProtos.MinorType type) {
    switch (type) {
      case VARCHAR:
      case VAR16CHAR:
      case VARBINARY:
      case DECIMAL9:
      case DECIMAL18:
      case DECIMAL28SPARSE:
      case DECIMAL38SPARSE:
      case INTERVALDAY:
        return holder;
      case BIT:
      case INT:
      case FLOAT4:
      case FLOAT8:
      case BIGINT:
      case INTERVALYEAR:
        return holder.ref("value");
      default:
        throw new IllegalArgumentException("Unsupported minor type of array: "+type.name());
    }
  }

  /**
   * Get ArrayMinorType from {@link TypeProtos.MinorType}
   * @param minorType minor type of the elements.
   * @return - a related ArrayMinorType
   */
  public static ArrayMinorType getElementType(TypeProtos.MinorType minorType) {
    switch (minorType) {
      case VARCHAR:
        return ArrayMinorType.VARCHAR;
      case INT:
        return ArrayMinorType.INTEGER;
      case FLOAT4:
        return ArrayMinorType.FLOAT;
      case FLOAT8:
        return ArrayMinorType.DOUBLE;
      case DECIMAL9:
        return ArrayMinorType.DECIMAL9;
      case DECIMAL18:
        return ArrayMinorType.DECIMAL18;
      case DECIMAL28SPARSE:
        return ArrayMinorType.DECIMAL28SPARSE;
      case DECIMAL38SPARSE:
        return ArrayMinorType.DECIMAL38SPARSE;
      case INTERVALYEAR:
        return ArrayMinorType.INTERVAL_YEAR_MONTH;
      case INTERVALDAY:
        return ArrayMinorType.INTERVAL_DAY_TIME;
      case BIT:
        return ArrayMinorType.BOOLEAN;
      case VARBINARY:
        return ArrayMinorType.BINARY;
      case BIGINT:
        return ArrayMinorType.BIGINT;
    }
    throw new IllegalArgumentException("Unsupported minor type of array: "+minorType.name());
  }

  /**
   * Copy array elements' value from {@link org.apache.drill.common.expression.ArrayValueConstructorExpression#input}.
   * Then assign the value to Janino variable.
   * @param it the array elements' expression
   */
  public void copyValueFromExp(JCodeModel model,
                               ClassGenerator generator,
                               JBlock eval,
                               JVar vv1,
                               JVar amType,
                               Iterator<ClassGenerator.HoldingContainer> it) {
    int index = 0;
    JType objType = model._ref(Object.class);
    JType baseType = model._ref(BaseValueVector.class);
    JVar obj = eval.decl(objType, generator.getNextVar("obj"));
    JVar baseV = eval.decl(baseType, generator.getNextVar("baseV"), vv1);
    while (it.hasNext()) {
      ClassGenerator.HoldingContainer next = it.next();
      TypeProtos.MinorType mt = next.getMinorType();
      if (next.isConstant()) {
        eval.assign(obj, next.getValue());
      } else if (next.isOptional()) {
        JFieldRef isSet = next.getIsSet();
        JConditional _if = eval._if(isSet.eq(JExpr.lit(1)));
        _if._then().assign(obj, assignJValue(next.getHolder(), mt));
        _if._else().assign(obj, JExpr._null());
      } else if(next.isRepeated()) {
        throw new IllegalArgumentException("Unsupported repeated mode in array, type: " + next.getMinorType());
      } else {
        eval.assign(obj, assignJValue(next.getHolder(), mt));
      }
      eval.invoke(amType, "addValueSafe").arg(obj).arg(baseV).arg(JExpr.lit(index));
      index++;
    }
  }


  public static final Set<Class<?>> ARRAY_ELEMENTS_CLASS = new ImmutableSet.Builder<Class<?>>()
    .add(Decimal28SparseHolder.class)
    .add(Decimal38SparseHolder.class)
    .add(RepeatedBigIntHolder.class)
    .add(RepeatedBitHolder.class)
    .add(RepeatedDecimal18Holder.class)
    .add(RepeatedDecimal28SparseHolder.class)
    .add(RepeatedDecimal38SparseHolder.class)
    .add(RepeatedDecimal9Holder.class)
    .add(RepeatedFloat4Holder.class)
    .add(RepeatedFloat8Holder.class)
    .add(RepeatedIntHolder.class)
    .add(RepeatedIntervalDayHolder.class)
    .add(RepeatedIntervalYearHolder.class)
    .add(RepeatedSmallIntHolder.class)
    .add(RepeatedTinyIntHolder.class)
    .add(RepeatedVarBinaryHolder.class)
    .add(RepeatedVarCharHolder.class)
    .build();

  public static final Set<Class<?>> VAR_ELEMENTS_CLASS = new ImmutableSet.Builder<Class<?>>()
    .add(RepeatedVarBinaryHolder.class)
    .add(RepeatedVarCharHolder.class)
    .add(RepeatedVar16CharHolder.class)
    .build();
}
