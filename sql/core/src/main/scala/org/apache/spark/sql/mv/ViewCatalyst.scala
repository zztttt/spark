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

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.apache.spark.sql.mv.optimizer.RewriteHelper

trait ViewCatalyst {
//  def registerMaterializedViewFromLogicalPlan(name: String, tableLogicalPlan: LogicalPlan, createLP: LogicalPlan): ViewCatalyst
//
//  def registerTableFromLogicalPlan(name: String, tableLogicalPlan: LogicalPlan): ViewCatalyst
//
//  def getCandidateViewsByTable(tableName: String): Option[Set[String]]
//
//  def getViewLogicalPlan(viewName: String): Option[LogicalPlan]
//
//  def getViewCreateLogicalPlan(viewName: String): Option[LogicalPlan]
//
//  def getViewNameByLogicalPlan(viewLP: LogicalPlan): Option[String]
//
//  def getTableNameByLogicalPlan(viewLP: LogicalPlan): Option[String]
}

class MaterializedViewCatalyst private extends ViewCatalyst with RewriteHelper {
  // view name -> LogicalPlan
  private val viewToCreateLogicalPlan = new java.util.concurrent.ConcurrentHashMap[String, LogicalPlan]()

  // view name -> LogicalPlan
  private val viewToLogicalPlan = new java.util.concurrent.ConcurrentHashMap[String, LogicalPlan]()

  // view name -> view create sql
  private val viewToCreateSql = new java.util.concurrent.ConcurrentHashMap[String, String]()

  // view name -> view checkpoint path
  private val viewToCheckpointPath = new java.util.concurrent.ConcurrentHashMap[String, String]()

  // table -> view
  private val tableToViews = new java.util.concurrent.ConcurrentHashMap[String, Set[String]]()

  // simple meta data for LogicalPlanSQL
  // logical plan of MV -> view name
  private val logicalPlanToTableName = new java.util.concurrent.ConcurrentHashMap[LogicalPlan, String]()

  def registerMaterializedViewFromLogicalPlan(name: String, tableLogicalPlan: LogicalPlan, createLP: LogicalPlan): MaterializedViewCatalyst = {
    def pushToTableToViews(tableName: String) = {
      val items = tableToViews.asScala.getOrElse(tableName, Set[String]())
      tableToViews.put(tableName, items ++ Set(name))
    }

    extractTablesFromPlan(createLP).foreach { tableName =>
      pushToTableToViews(tableName)
    }

    viewToCreateLogicalPlan.put(name, createLP)
    viewToLogicalPlan.put(name, tableLogicalPlan)
    this
  }

  // view name -> view checkpoint path
  def registerMaterializedViewFromCreateSql(name: String, createSql: String): Unit = {
    viewToCreateSql.put(name, createSql)
  }

  // view name -> view checkpoint path
  def registerMaterializedViewFromCheckpointPath(name: String, path: String): Unit = {
    viewToCheckpointPath.put(name, path)
  }

  def registerTableFromLogicalPlan(name: String, tableLogicalPlan: LogicalPlan): MaterializedViewCatalyst = {
    logicalPlanToTableName.put(tableLogicalPlan, name)
    this
  }


  def getCandidateViewsByTable(tableName: String): Option[Set[String]] = {
    tableToViews.asScala.get(tableName)
  }

  def getViewLogicalPlan(viewName: String): Option[LogicalPlan] = {
    viewToLogicalPlan.asScala.get(viewName)
  }

  def getViewCreateLogicalPlan(viewName: String): Option[LogicalPlan] = {
    viewToCreateLogicalPlan.asScala.get(viewName)
  }

  def getViewNameByLogicalPlan(viewLP: LogicalPlan): Option[String] = {
    viewToLogicalPlan.asScala.filter(f => f._2 == viewLP).keys.headOption
  }

  def getTableNameByLogicalPlan(viewLP: LogicalPlan): Option[String] = {
    logicalPlanToTableName.asScala.get(viewLP)
  }
}

case class TableHolder(db: String, table: String, output: Seq[NamedExpression], lp: LogicalPlan)

object MaterializedViewCatalyst {
  private val materializedViewCatalyst = new MaterializedViewCatalyst

  def getInstance(): MaterializedViewCatalyst = {
    materializedViewCatalyst
  }
}
