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
package org.apache.spark.sql.mv

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias

class SchemaRegistry(_spark: SparkSession) {
  val spark = _spark.cloneSession()

  def createTableInHudi(tableName: String, sql: String): ViewCatalyst = {
    spark.sql(sql)
    val logicalPlan = spark.table(tableName).queryExecution.analyzed match {
      case a@SubqueryAlias(_, child) => child
      case a@_ => a
    }
    ViewCatalyst.meta.registerTableFromLogicalPlan(tableName, logicalPlan)
  }

  def createMV(viewName: String, viewCreate: String) = {
    val createViewTable1 = spark.sql(viewCreate)
    val df = spark.createDataFrame(createViewTable1.rdd, createViewTable1.schema)
    df.createOrReplaceTempView(viewName)
    ViewCatalyst.meta.registerTableFromLogicalPlan(viewName, df.queryExecution.analyzed)
    ViewCatalyst.meta.registerMaterializedViewFromLogicalPlan(viewName, df.queryExecution.analyzed, createViewTable1.queryExecution.analyzed)
  }

  def toLogicalPlan(sql: String) = {
    val temp = spark.sql(sql).queryExecution.analyzed
    temp
  }

//  def genSQL(lp: LogicalPlan) = {
//    val temp = new LogicalPlanSQL(lp, new BasicSQLDialect).toSQL
//    temp
//  }

//  def genPrettySQL(lp: LogicalPlan) = {
//    SQLUtils.format(genSQL(lp), JdbcConstants.HIVE)
//  }
}
