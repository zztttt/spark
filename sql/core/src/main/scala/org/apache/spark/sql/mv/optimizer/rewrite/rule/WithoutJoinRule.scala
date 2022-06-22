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
package org.apache.spark.sql.mv.optimizer.rewrite.rule

import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.mv.MaterializedViewCatalyst
import org.apache.spark.sql.mv.optimizer.rewrite.component.{AggMatcher, GroupByMatcher, PredicateMatcher, TableNonOpMatcher}
import org.apache.spark.sql.mv.optimizer.rewrite.component.rewrite.{AggRewrite, GroupByRewrite, PredicateRewrite, TableOrViewRewrite}

class WithoutJoinRule extends RewriteMatchRule {


  override def fetchView(plan: LogicalPlan, rewriteContext: RewriteContext): Seq[ViewLogicalPlan] = {
    require(plan.resolved, "LogicalPlan must be resolved.")
    if (isJoinExists(plan)) return Seq()

    val tables = extractTablesFromPlan(plan)
    if (tables.isEmpty)
      return Seq()
    val table = tables.head
    val viewPlan = MaterializedViewCatalyst.getInstance().getCandidateViewsByTable(table) match {
      case Some(viewNames) =>
        viewNames.filter { viewName =>
          MaterializedViewCatalyst.getInstance().getViewCreateLogicalPlan(viewName) match {
            case Some(viewLogicalPlan) =>
              extractTablesFromPlan(viewLogicalPlan).toSet == Set(table)
            case None => false
          }
        }.map { targetViewName =>
          ViewLogicalPlan(
            MaterializedViewCatalyst.getInstance().getViewLogicalPlan(targetViewName).get,
            MaterializedViewCatalyst.getInstance().getViewCreateLogicalPlan(targetViewName).get)
        }.toSeq
      case None => Seq()


    }
    viewPlan
  }

  override def rewrite(plan: LogicalPlan, rewriteContext: RewriteContext): LogicalPlan = {
    val targetViewPlanOption = fetchView(plan, rewriteContext)
    if (targetViewPlanOption.isEmpty) return plan

    var shouldBreak = false
    var finalPlan = RewritedLogicalPlan(plan, true)

    targetViewPlanOption.foreach { targetViewPlan =>
      if (!shouldBreak) {
        rewriteContext.viewLogicalPlan.set(targetViewPlan)
        val res = _rewrite(plan, rewriteContext)
        res match {
          case a@RewritedLogicalPlan(_, true) =>
            finalPlan = a
          case a@RewritedLogicalPlan(_, false) =>
            finalPlan = a
            shouldBreak = true
        }
      }
    }
    finalPlan
  }

  def _rewrite(plan: LogicalPlan, rewriteContext: RewriteContext): LogicalPlan = {

    generateRewriteContext(plan, rewriteContext)
    /**
     * Three match/rewrite steps:
     *   1. Predicate
     *   2. GroupBy
     *   3. Project
     *   4. Table(View)
     */
    val pipeline = buildPipeline(rewriteContext: RewriteContext, Seq(
      new PredicateMatcher(rewriteContext),
      new PredicateRewrite(rewriteContext),
      new GroupByMatcher(rewriteContext),
      new GroupByRewrite(rewriteContext),
      new AggMatcher(rewriteContext),
      new AggRewrite(rewriteContext),
      new TableNonOpMatcher(rewriteContext),
      new TableOrViewRewrite(rewriteContext)
    ))

    /**
     * When we are rewriting plan, any step fails, we should return the original plan.
     * So we should check the mark in RewritedLogicalPlan is final success or fail.
     */
    LogicalPlanRewritePipeline(pipeline).rewrite(plan)
  }
}

object WithoutJoinRule {
  def apply: WithoutJoinRule = new WithoutJoinRule()
}

