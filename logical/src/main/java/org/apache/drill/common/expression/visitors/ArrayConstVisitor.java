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
package org.apache.drill.common.expression.visitors;

import org.apache.drill.common.expression.LogicalExpression;
import org.apache.drill.common.expression.ValueExpressions;

/**
 * Visitor for array constant.
 */

public class ArrayConstVisitor extends AbstractExprVisitor<Object, Void, RuntimeException> {

  public static final ArrayConstVisitor INSTANCE = new ArrayConstVisitor();

  private ArrayConstVisitor(){}

  @Override
  public Object visitUnknown(LogicalExpression e, Void value) throws RuntimeException {
    throw new UnsupportedOperationException("Can't visit the value of "+ e.getMajorType());
  }

  @Override
  public Object visitFloatConstant(ValueExpressions.FloatExpression fExpr, Void value) throws RuntimeException {
    return fExpr.getFloat();
  }

  @Override
  public Object visitIntConstant(ValueExpressions.IntExpression intExpr, Void value) throws RuntimeException {
    return intExpr.getInt();
  }

  @Override
  public Object visitLongConstant(ValueExpressions.LongExpression intExpr, Void value) throws RuntimeException {
    return intExpr.getLong();
  }

  @Override
  public Object visitDecimal9Constant(ValueExpressions.Decimal9Expression decExpr, Void value) throws RuntimeException {
    return decExpr;
  }

  @Override
  public Object visitDecimal18Constant(ValueExpressions.Decimal18Expression decExpr, Void value) throws RuntimeException {
    return decExpr;
  }

  @Override
  public Object visitDecimal28Constant(ValueExpressions.Decimal28Expression decExpr, Void value) throws RuntimeException {
    return decExpr.getBigDecimal().toString();
  }

  @Override
  public Object visitDecimal38Constant(ValueExpressions.Decimal38Expression decExpr, Void value) throws RuntimeException {
    return decExpr.getBigDecimal().toString();
  }

  @Override
  public Object visitIntervalYearConstant(ValueExpressions.IntervalYearExpression intExpr, Void value) throws RuntimeException {
    return intExpr.getIntervalYear();
  }

  @Override
  public Object visitIntervalDayConstant(ValueExpressions.IntervalDayExpression intExpr, Void value) throws RuntimeException {
    return new int[]{intExpr.getIntervalDay(), intExpr.getIntervalMillis()};
  }

  @Override
  public Object visitDoubleConstant(ValueExpressions.DoubleExpression dExpr, Void value) throws RuntimeException {
    return dExpr.getDouble();
  }

  @Override
  public Object visitBooleanConstant(ValueExpressions.BooleanExpression e, Void value) throws RuntimeException {
    return e.getBoolean();
  }

  @Override
  public Object visitQuotedStringConstant(ValueExpressions.QuotedString e, Void value) throws RuntimeException {

    return e.getString();
  }


}
