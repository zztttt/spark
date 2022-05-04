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

import org.apache.spark.sql.mv.optimizer.rewrite.component.utils.{ExpressionIntersectResp, ExpressionSemanticEquals}
import org.apache.spark.sql.mv.optimizer.rewrite.rule.{CompensationExpressions, ExpressionMatcher, RewriteContext, RewriteFail}

class ProjectMatcher(rewriteContext: RewriteContext) extends ExpressionMatcher {
  /**
   *
   * @param query the project expression list in query
   * @param view  the project expression list in view
   * @return
   *
   * We should make sure all query project list isSubSet of view project list.
   *
   *
   */
  override def compare: CompensationExpressions = {

    val query = rewriteContext.processedComponent.get().queryProjectList
    val view = rewriteContext.processedComponent.get().viewProjectList
    val ExpressionIntersectResp(queryLeft, viewLeft, _) = ExpressionSemanticEquals.process(query, view)
    // for now, we must make sure the queryLeft's columns(not alias) all in viewLeft.columns(not alias)
    val queryColumns = queryLeft.flatMap(extractAttributeReference)
    val viewColumns = viewLeft.flatMap(extractAttributeReference)

    val ExpressionIntersectResp(queryColumnsLeft, viewColumnsLeft, _) = ExpressionSemanticEquals.process(queryColumns, viewColumns)
    if (queryColumnsLeft.size > 0) return RewriteFail.PROJECT_UNMATCH(this)
    CompensationExpressions(true, Seq())
  }


}

