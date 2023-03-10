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
package org.apache.spark.sql.mv.optimizer

import org.apache.spark.sql.catalyst.expressions.{AttributeReference, PredicateHelper}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project, SubqueryAlias, Union}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.mv.optimizer.rewrite.rule.{ProcessedComponent, RewriteContext, RewriteMatchRule, RewritedLogicalPlan, SPGJRule, ViewLogicalPlan, WithoutJoinGroupRule, WithoutJoinRule}

import java.util.concurrent.atomic.AtomicReference
import scala.collection.mutable.ArrayBuffer

object RewriteTableToViews extends Rule[LogicalPlan] with PredicateHelper {
  val batches = ArrayBuffer[RewriteMatchRule](
    WithoutJoinGroupRule.apply,
    WithoutJoinRule.apply,
    SPGJRule.apply
  )

  def apply(plan: LogicalPlan): LogicalPlan = {
    var lastPlan = plan
    var shouldStop = false
    var count = 1
    val rewriteContext = new RewriteContext(new AtomicReference[ViewLogicalPlan](), new AtomicReference[ProcessedComponent]())
    while (!shouldStop && count > 0) {
      count -= 1
      var currentPlan = if (isSPJG(plan)) {
        rewrite(plan, rewriteContext)
      } else {
        plan.transformUp {
          case a if isSPJG(a) =>
            rewrite(a, rewriteContext)
        }
      }
      if (currentPlan != lastPlan) {
        // fix all attributeRef in finalPlan
        currentPlan = currentPlan transformAllExpressions {
          case ar@AttributeReference(_, _, _, _) =>
            val qualifier = ar.qualifier
            rewriteContext.replacedARMapping.getOrElse(ar.withQualifier(Seq()), ar).withQualifier(qualifier)
        }
      } else {
        shouldStop = true
      }
      lastPlan = currentPlan
    }
    lastPlan
  }

  private def rewrite(plan: LogicalPlan, rewriteContext: RewriteContext) = {
    // this plan is SPJG, but the first step is check whether we can rewrite it
    var rewritePlan = plan
    batches.foreach { rewriter =>
      val start = System.currentTimeMillis()
      rewritePlan = rewriter.rewrite(rewritePlan, rewriteContext)
      logWarning("rewriter time:" + String.valueOf(System.currentTimeMillis() - start))
    }

    rewritePlan match {
      case RewritedLogicalPlan(_, true) =>
        logWarning(s"=====try to rewrite but fail ======:\n\n${plan} ")
        plan
      case RewritedLogicalPlan(inner, false) =>
        logWarning(s"=====try to rewrite and success ======:\n\n${plan}  \n\n ${inner}")
        inner
      case _ =>
        logWarning(s"=====try to rewrite but fail ======:\n\n${plan} ")
        rewritePlan
    }
  }

  /**
   * check the plan is whether a basic sql pattern
   * only contains select(filter)/agg/project/join/group.
   *
   * @param plan
   * @return
   */
  private def isSPJG(plan: LogicalPlan): Boolean = {
    var isMatch = true
    plan transformDown {
      case a@SubqueryAlias(_, Project(_, _)) =>
        isMatch = false
        a
      case a@Union(_, _, _) =>
        isMatch = false
        a
    }

    if (!isMatch) {
      return false
    }

    plan match {
      case p@Project(_, Join(_, _, _, _, _)) => true
      case p@Project(_, Filter(_, Join(_, _, _, _, _))) => true
      case p@Aggregate(_, _, Filter(_, Join(_, _, _, _, _))) => true
      case p@Aggregate(_, _, Filter(_, _)) => true
      case p@Aggregate(_, _, Join(_, _, _, _, _)) => true
      case p@Aggregate(_, _, SubqueryAlias(_, LogicalRDD(_, _, _, _, _))) => true
      case p@Aggregate(_, _, SubqueryAlias(_, LogicalRelation(_, _, _, _))) => true
      case p@Project(_, SubqueryAlias(_, LogicalRDD(_, _, _, _, _))) => true
      case p@Project(_, SubqueryAlias(_, LogicalRelation(_, _, _, _))) => true
      case p@Project(_, Filter(_, Aggregate(_, _, _))) => false // for having
      case _ => false
    }
  }
}
