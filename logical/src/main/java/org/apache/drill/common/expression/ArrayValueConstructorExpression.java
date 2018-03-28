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
package org.apache.drill.common.expression;

import com.google.common.collect.ImmutableList;
import org.apache.drill.common.expression.visitors.ExprVisitor;
import org.apache.drill.common.types.TypeProtos;
import org.apache.drill.common.types.Types;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Iterator;
import java.util.List;

/**
 * An expression for Array Value Constructor.
 * @see org.apache.calcite.sql.SqlKind#ARRAY_VALUE_CONSTRUCTOR
 */

public class ArrayValueConstructorExpression
  extends LogicalExpressionBase
  implements Iterable<LogicalExpression> {

  private static final Logger logger = LoggerFactory.getLogger(ArrayValueConstructorExpression.class);

  private final List<LogicalExpression> input;
  private TypeProtos.MajorType type;

  protected ArrayValueConstructorExpression(TypeProtos.MajorType type, ExpressionPosition pos, List<LogicalExpression> input) {
    super(pos);
    this.type = type;
    this.input = input;
  }

  @Override
  public <T, V, E extends Exception> T accept(ExprVisitor<T, V, E> visitor, V value) throws E {

    return visitor.visitArrayValueConstructor(this, value);
  }

  public ImmutableList<LogicalExpression> immutableArgs() {
    return ImmutableList.copyOf(input);
  }

  public void resetArg(TypeProtos.MinorType type, LogicalExpression e, int index) {
    input.set(index, e);
    mayResetType(type);
  }

  private void mayResetType(TypeProtos.MinorType newType) {
    TypeProtos.MinorType oldType = type.getMinorType();
    if (oldType == TypeProtos.MinorType.LATE) {
      type = Types.repeated(newType);
    } else if (oldType != TypeProtos.MinorType.VARCHAR && newType == TypeProtos.MinorType.VARCHAR){
      type = Types.repeated(TypeProtos.MinorType.VARCHAR);
    } else if (oldType != newType){
      switch (newType) {
        case BIT:
          switch (oldType) {
            case TINYINT:
            case SMALLINT:
            case INT:
            case FLOAT4:
            case FLOAT8:
            case BIGINT:
              break;
            default:
              throw new UnsupportedOperationException("Unsupported type reset for old["+oldType+"] and new["+newType+"]");
          }
          break;
        case TINYINT:
          switch (oldType) {
            case BIT:
              type = Types.repeated(TypeProtos.MinorType.TINYINT);
              break;
            case SMALLINT:
            case INT:
            case FLOAT4:
            case FLOAT8:
            case BIGINT:
              break;
            default:
              throw new UnsupportedOperationException("Unsupported type reset for old["+oldType+"] and new["+newType+"]");
          }
          break;
        case SMALLINT:
          switch (oldType) {
            case BIT:
            case TINYINT:
              type = Types.repeated(TypeProtos.MinorType.SMALLINT);
              break;
            case INT:
            case FLOAT4:
            case FLOAT8:
            case BIGINT:
              break;
            default:
              throw new UnsupportedOperationException("Unsupported type reset for old["+oldType+"] and new["+newType+"]");
          }
          break;
        case INT:
          switch (oldType) {
            case BIT:
            case TINYINT:
            case SMALLINT:
              type = Types.repeated(TypeProtos.MinorType.INT);
              break;
            case FLOAT4:
            case FLOAT8:
            case BIGINT:
              break;
            default:
              throw new UnsupportedOperationException("Unsupported type reset for old["+oldType+"] and new["+newType+"]");
          }
          break;
        case FLOAT4:
          switch (oldType) {
            case BIT:
            case TINYINT:
            case SMALLINT:
            case INT:
              type = Types.repeated(TypeProtos.MinorType.FLOAT4);
              break;
            case BIGINT:
              type = Types.repeated(TypeProtos.MinorType.FLOAT8);
              break;
            case FLOAT8:
              break;
            default:
              throw new UnsupportedOperationException("Unsupported type reset for old["+oldType+"] and new["+newType+"]");
          }
          break;
        case FLOAT8:
          switch (oldType) {
            case BIT:
            case TINYINT:
            case SMALLINT:
            case INT:
            case FLOAT4:
            case BIGINT:
              type = Types.repeated(TypeProtos.MinorType.FLOAT8);
              break;
            default:
              throw new UnsupportedOperationException("Unsupported type reset for old["+oldType+"] and new["+newType+"]");
          }
          break;
        case BIGINT:
          switch (oldType) {
            case BIT:
            case TINYINT:
            case SMALLINT:
            case INT:
            case FLOAT4:
              type = Types.repeated(TypeProtos.MinorType.BIGINT);
              break;
            case FLOAT8:
              break;
            default:
              throw new UnsupportedOperationException("Unsupported type reset for old["+oldType+"] and new["+newType+"]");
          }
          break;
          default:
            throw new UnsupportedOperationException("Unsupported type reset for old["+oldType+"] and new["+newType+"]");

      }
    }

  }

  @Override
  public Iterator<LogicalExpression> iterator() {
    return input.iterator();
  }

  @Override
  public TypeProtos.MajorType getMajorType() {
    return this.type;
  }

  public int valueSize() {
    return input.size();
  }

}
