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
package org.apache.spark.sql.mv.optimizer.rewrite.component.utils

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.mv.optimizer.RewriteHelper

object ExpressionSemanticEquals extends RewriteHelper {
  def process(query: Seq[Expression], view: Seq[Expression]) = {
    val (viewLeft, queryLeft, common) = extractTheSameExpressions(view, query)
    ExpressionIntersectResp(queryLeft, viewLeft, common)
  }
}

case class ExpressionIntersectResp(
                                    queryLeft: Seq[Expression],
                                    viewLeft: Seq[Expression],
                                    common: Seq[Expression]
                                  )

