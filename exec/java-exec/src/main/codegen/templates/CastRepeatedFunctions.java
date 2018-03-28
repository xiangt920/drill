/**
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
<@pp.dropOutputFile />



<#list castrepeated.types as from>
<#list castrepeated.types as to>

<#if (!from.name.equals(to.name))>
<@pp.changeOutputFile name="/org/apache/drill/exec/expr/fn/impl/gcast/CastRepeated${from.name}${to.name}.java" />

<#include "/@includes/license.ftl" />

package org.apache.drill.exec.expr.fn.impl.gcast;

import org.apache.drill.exec.expr.DrillSimpleFunc;
import org.apache.drill.exec.expr.annotations.FunctionTemplate;
import org.apache.drill.exec.expr.annotations.FunctionTemplate.NullHandling;
import org.apache.drill.exec.expr.annotations.Output;
import org.apache.drill.exec.expr.annotations.Param;
import org.apache.drill.exec.expr.holders.*;
import org.apache.drill.exec.record.RecordBatch;

/*
 * This class is generated using freemarker and the ${.template_name} template.
 */

@SuppressWarnings("unused")
@FunctionTemplate(name = "cast${to.minor}", scope = FunctionTemplate.FunctionScope.SIMPLE, nulls=NullHandling
  .NULL_IF_NULL, isRandom = true)
public class CastRepeated${from.name}${to.name} implements DrillSimpleFunc{

  @Param Repeated${from.name}Holder in;
  @Output Repeated${to.name}Holder out;

  public void setup() {}

  public void eval() {
    for (int i = in.start; i < in.end; i++) {
      out.vector.getMutator().setSafe(i - in.start, (${to.native})in.vector.getAccessor().get(i));
    }
    out.end = in.end - in.start;
  }
}

</#if>
</#list>
</#list>

