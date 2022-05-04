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
    var count = 100
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
        //fix all attributeRef in finalPlan
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
      rewritePlan = rewriter.rewrite(rewritePlan, rewriteContext)
    }

    rewritePlan match {
      case RewritedLogicalPlan(_, true) =>
        logInfo(s"=====try to rewrite but fail ======:\n\n${plan} ")
        plan
      case RewritedLogicalPlan(inner, false) =>
        logInfo(s"=====try to rewrite and success ======:\n\n${plan}  \n\n ${inner}")
        inner
      case _ =>
        logInfo(s"=====try to rewrite but fail ======:\n\n${plan} ")
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
    println(plan)
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
      case p@Project(_, Filter(_, _)) => true
      case p@Aggregate(_, _, Join(_, _, _, _, _)) => true
      case p@Aggregate(_, _, SubqueryAlias(_, LogicalRDD(_, _, _, _, _))) => true
      case p@Aggregate(_, _, SubqueryAlias(_, LogicalRelation(_, _, _, _))) => true
      case p@Project(_, SubqueryAlias(_, LogicalRDD(_, _, _, _, _))) => true
      case p@Project(_, SubqueryAlias(_, LogicalRelation(_, _, _, _))) => true
      case _ => false
    }
  }
}
