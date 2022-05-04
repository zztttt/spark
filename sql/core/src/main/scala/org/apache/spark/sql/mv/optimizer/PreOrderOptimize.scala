package org.apache.spark.sql.mv.optimizer

import org.apache.spark.sql.catalyst.optimizer.{EliminateOuterJoin, PushPredicateThroughJoin}
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.catalyst.rules.RuleExecutor

object PreOptimizeRewrite extends RuleExecutor[LogicalPlan] {
  val batches = Batch("Before join rewrite", FixedPoint(100), EliminateOuterJoin, PushPredicateThroughJoin) :: Nil
}
