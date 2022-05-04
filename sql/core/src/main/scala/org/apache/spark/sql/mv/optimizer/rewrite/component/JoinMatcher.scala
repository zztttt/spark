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
package org.apache.spark.sql.mv.optimizer.rewrite.component

import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{Filter, Join, SubqueryAlias}
import org.apache.spark.sql.mv.optimizer.rewrite.rule.{CompensationExpressions, ExpressionMatcher, RewriteContext, RewriteFail}

class JoinMatcher(rewriteContext: RewriteContext) extends ExpressionMatcher {
  override def compare: CompensationExpressions = {


    val viewJoin = rewriteContext.processedComponent.get().viewJoins.head
    val queryJoin = rewriteContext.processedComponent.get().queryJoins.head
    // since the prediate condition will be pushed down into Join filter,
    // but we have compare them in Predicate Matcher/Rewrite step, so when compare Join,
    // we should clean the filter from Join
    if (!sameJoinPlan(cleanJoinFilter(viewJoin), cleanJoinFilter(queryJoin))) return RewriteFail.JOIN_UNMATCH(this)
    CompensationExpressions(true, Seq())
  }

  def cleanJoinFilter(join: Join) = {
    val newPlan = join transformUp {
      case a@Filter(_, child) =>
        child
      case SubqueryAlias(_, a@SubqueryAlias(_, _)) =>
        a
      case a@Join(_, _, _, condition, _) =>
        if (condition.isDefined) {
          val newConditions = condition.get transformUp {
            case a@AttributeReference(name, dataType, nullable, metadata) =>
              AttributeReference(name, dataType, nullable, metadata)(a.exprId, Seq())
          }
          a.copy(condition = Option(newConditions))

        } else a

    }
    newPlan
  }

}
