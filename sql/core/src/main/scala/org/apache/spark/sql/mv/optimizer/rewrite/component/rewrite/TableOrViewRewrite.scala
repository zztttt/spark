/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.mv.optimizer.rewrite.component.rewrite

import org.apache.spark.sql.catalyst.catalog.HiveTableRelation
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.mv.optimizer.rewrite.rule.{LogicalPlanRewrite, RewriteContext, RewritedLeafLogicalPlan}

class TableOrViewRewrite(rewriteContext: RewriteContext) extends LogicalPlanRewrite {
  override def rewrite(plan: LogicalPlan): LogicalPlan = {
    val finalTable = rewriteContext.viewLogicalPlan.get().tableLogicalPlan match {
      case Project(_, child) => child
      case _ => rewriteContext.viewLogicalPlan.get().tableLogicalPlan
    }
    val newPlan = plan transformDown {
      case SubqueryAlias(_, _) =>
        RewritedLeafLogicalPlan(finalTable)
      case HiveTableRelation(_, _, _, _, _) =>
        RewritedLeafLogicalPlan(finalTable)
      case LogicalRelation(_, output, catalogTable, _) =>
        RewritedLeafLogicalPlan(finalTable)
    }

    _back(newPlan)

  }
}
