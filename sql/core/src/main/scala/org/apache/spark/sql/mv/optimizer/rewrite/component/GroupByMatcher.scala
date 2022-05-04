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

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.mv.optimizer.rewrite.rule.{CompensationExpressions, ExpressionMatcher, RewriteContext, RewriteFail}

import scala.collection.mutable.ArrayBuffer

class GroupByMatcher(rewriteContext: RewriteContext) extends ExpressionMatcher {
  override def compare: CompensationExpressions = {
    /**
     * Query:
     *
     * SELECT deptno
     * FROM emps
     * WHERE deptno > 10
     * GROUP BY deptno
     *
     * View:
     *
     * SELECT empid, deptno
     * FROM emps
     * WHERE deptno > 5
     * GROUP BY empid, deptno
     *
     * Target:
     *
     * SELECT deptno
     * FROM mv
     * WHERE deptno > 10
     * GROUP BY deptno
     *
     * then  query  isSubSet of view . Please take care of the order in group by.
     */
    val query = rewriteContext.processedComponent.get().queryGroupingExpressions
    val view = rewriteContext.processedComponent.get().viewGroupingExpressions
    val viewAggregateExpressions = rewriteContext.processedComponent.get().viewAggregateExpressions

    if (query.size > view.size) return RewriteFail.GROUP_BY_SIZE_UNMATCH(this)
    if (!isSubSetOf(query, view)) return RewriteFail.GROUP_BY_SIZE_UNMATCH(this)

    // again make sure the columns in queryLeft is also in view project/agg

    val viewAttrs = extractAttributeReferenceFromFirstLevel(viewAggregateExpressions)

    val compensationCondAllInViewProjectList = isSubSetOf(query.flatMap(extractAttributeReference), viewAttrs)

    if (!compensationCondAllInViewProjectList) return RewriteFail.GROUP_BY_COLUMNS_NOT_IN_VIEW_PROJECT_OR_AGG(this)

    CompensationExpressions(true, query)

  }

  private def extractTheSameExpressionsOrder(view: Seq[Expression], query: Seq[Expression]) = {
    val viewLeft = ArrayBuffer[Expression](view: _*)
    val queryLeft = ArrayBuffer[Expression](query: _*)
    val common = ArrayBuffer[Expression]()

    (0 until view.size).foreach { index =>
      if (view(index).semanticEquals(query(index))) {
        common += view(index)
        viewLeft -= view(index)
        queryLeft -= query(index)
      }
    }

    (viewLeft, queryLeft, common)
  }
}
