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
package org.apache.spark.sql.mv.sqlGenerator

import java.sql.Connection
import org.apache.spark.sql.catalyst.dsl.plans._
import org.apache.spark.sql.catalyst.plans.logical.Aggregate
import org.apache.spark.sql.execution.LogicalRDD
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.mv.MaterializedViewCatalyst

class BasicSQLDialect extends SQLDialect {
  override def canHandle(url: String): Boolean = url.toLowerCase().startsWith("jdbc:mysql")

  override def quote(name: String): String = {
    "`" + name.replace("`", "``") + "`"
  }

  override def explainSQL(sql: String): String = s"EXPLAIN $sql"

  override def relation(relation: LogicalRelation): String = {
    val view = relation.select(relation.output: _*)
    MaterializedViewCatalyst.getInstance().getViewNameByLogicalPlan(view) match {
      case Some(i) => i
      case None => MaterializedViewCatalyst.getInstance().getTableNameByLogicalPlan(relation.logicalPlan) match {
        case Some(i) => i
        case None => null
      }
    }
  }

  override def relation2(relation: LogicalRDD): String = {
    MaterializedViewCatalyst.getInstance().getTableNameByLogicalPlan(relation.logicalPlan) match {
      case Some(i) => i
      case None => null
    }
  }

  override def relation3(relation: Aggregate): String = {
    System.out.println(relation.child)
    MaterializedViewCatalyst.getInstance().getTableNameByLogicalPlan(relation.child) match {
      case Some(i) => i
      case None => null
    }
  }

  override def maybeQuote(name: String): String = {
    name
  }

  override def getIndexes(conn: Connection, url: String, tableName: String): Set[String] = {
    Set()
  }

  override def getTableStat(conn: Connection, url: String, tableName: String): (Option[BigInt], Option[Long]) = {
    (None, None)
  }

  override def enableCanonicalize: Boolean = false


}
