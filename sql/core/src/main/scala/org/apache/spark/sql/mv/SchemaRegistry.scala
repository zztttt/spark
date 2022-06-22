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

import com.google.gson.{Gson, JsonArray, JsonObject}
import org.apache.hadoop.fs.Path
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.{CheckPointReflector, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.SubqueryAlias
import org.apache.spark.sql.types.{DataType, FloatType, IntegerType, LongType, StringType, StructType}

import java.util.Properties

class SchemaRegistry(_spark: SparkSession) extends Logging {
  val spark = _spark

  def createTableInHudi(tableName: String, sql: String): MaterializedViewCatalyst = {
    spark.sql(sql)
    val logicalPlan = spark.table(tableName).queryExecution.analyzed match {
      case a@SubqueryAlias(_, child) => child
      case a@_ => a
    }
    MaterializedViewCatalyst.getInstance().registerTableFromLogicalPlan(tableName, logicalPlan)
  }


  def loadCkptMV(spark: SparkSession, viewName: String): DataFrame = {
    val checkpointPath = spark.sparkContext.getCheckpointDir
    if (!checkpointPath.isDefined) {
      throw new SparkException("check point dir is not set")
    }
    val viewPath = new Path(spark.sparkContext.getCheckpointDir.get, viewName)
    val fs = viewPath.getFileSystem(spark.sessionState.newHadoopConf())
    if (!fs.exists(viewPath)) {
      throw new SparkException("materialized view file does not exist:" + viewName)
    }

    logInfo("load checkpoint:" + viewPath)
    val metadata: JsonObject = MetadataClient.loadMV(viewName)
    // println(metadata.toString)
    val gson = new Gson()
    val viewCreateSql: String = metadata.get("createSql").getAsString
    val types = gson.fromJson(metadata.get("columnTypes").getAsString, classOf[JsonArray])
    var schema = new StructType()
    for(i <- 0 to types.size() - 1) {
      val column = types.get(i).getAsJsonObject
      schema = schema.add(column.get("name").getAsString, StringToDataType(column.get("type").getAsString))
      println(schema.toDDL)
    }

    val rdd = CheckPointReflector.recover[InternalRow](spark, viewPath.toString)
    val df = CheckPointReflector.createDataFrame(spark, rdd, schema)
    val viewTable = spark.sql(viewCreateSql)
    MaterializedViewCatalyst.getInstance().registerMaterializedViewFromLogicalPlan(viewName, df.queryExecution.withCachedData, viewTable.queryExecution.withCachedData)
    // register rdd dataframe
    MaterializedViewCatalyst.getInstance().registerTableFromLogicalPlan(viewName, df.queryExecution.analyzed)
    df
  }

  def loadHiveMV(spark: SparkSession, viewName: String): DataFrame = {
    logInfo("load checkpoint:" + viewName)
    // 1. load metadata
    val metadata: JsonObject = MetadataClient.loadMV(viewName)
    val viewCreateSql: String = metadata.get("createSql").getAsString
    val types = new Gson().fromJson(metadata.get("columnTypes").getAsString, classOf[JsonArray])
    var schema = new StructType()
    for(i <- 0 to types.size() - 1) {
      val column = types.get(i).getAsJsonObject
      schema = schema.add(column.get("name").getAsString, StringToDataType(column.get("type").getAsString))
      // println(schema.toDDL)
    }
    // 2. switch database and load dataframe
    // spark.catalog.listDatabases().show()
    // spark.catalog.setCurrentDatabase("mv")
    val viewTable = spark.sql(viewCreateSql)
    val df = spark.sql("select * from mv." + viewName)
    MaterializedViewCatalyst.getInstance().registerMaterializedViewFromLogicalPlan(viewName, df.queryExecution.withCachedData, viewTable.queryExecution.withCachedData)
    // register rdd dataframe
    MaterializedViewCatalyst.getInstance().registerTableFromLogicalPlan(viewName, df.queryExecution.analyzed)
    df
  }

  // useless
  // viewCreateSql will be moved in the future
  def loadCkptMV(spark: SparkSession, viewName: String, viewCreateSql: String): DataFrame = {
    val checkpointPath = spark.sparkContext.getCheckpointDir
    if (!checkpointPath.isDefined) {
      throw new SparkException("check point dir is not set")
    }

    val viewPath = new Path(spark.sparkContext.getCheckpointDir.get, viewName)
    val fs = viewPath.getFileSystem(spark.sessionState.newHadoopConf())
    if (!fs.exists(viewPath)) {
      throw new SparkException("materialized view file does not exist:" + viewName)
    }

    // load checkpoint
    logInfo("load checkpoint:" + viewPath)
    // TODO: manage mv metadata
    if (viewName.equals("emps_large_mv")) {
      val schema = new StructType()
        .add("EMPNO", IntegerType)
      val rdd = CheckPointReflector.recover[InternalRow](spark, viewPath.toString)
      val df = CheckPointReflector.createDataFrame(spark, rdd, schema)
      val viewTable = spark.sql(viewCreateSql)
      // viewTable is the original logical plan AST tree, and df is another LogicalRDD
      // MaterializedViewCatalyst.getInstance().registerMaterializedViewFromCheckpointPath(viewName, viewPath.toString)
      // MaterializedViewCatalyst.getInstance().registerMaterializedViewFromCreateSql(viewName, viewCreateSql)
      MaterializedViewCatalyst.getInstance().registerMaterializedViewFromLogicalPlan(viewName, df.queryExecution.withCachedData, viewTable.queryExecution.withCachedData)
      // register rdd dataframe
      MaterializedViewCatalyst.getInstance().registerTableFromLogicalPlan(viewName, df.queryExecution.analyzed)
      df
    } else if (viewName.equals("emps_large_agg_mv")) {
      val schema = new StructType()
        .add("EMPNO", IntegerType)
        .add("DEPTNO", IntegerType)
        .add("c", IntegerType)
        .add("s", IntegerType)
      val rdd = CheckPointReflector.recover[InternalRow](spark, viewPath.toString)
      val df = CheckPointReflector.createDataFrame(spark, rdd, schema)
      val viewTable = spark.sql(viewCreateSql)
      // viewTable is the original logical plan AST tree, and df is another LogicalRDD
      // MaterializedViewCatalyst.getInstance().registerMaterializedViewFromCheckpointPath(viewName, viewPath.toString)
      // MaterializedViewCatalyst.getInstance().registerMaterializedViewFromCreateSql(viewName, viewCreateSql)
      MaterializedViewCatalyst.getInstance().registerMaterializedViewFromLogicalPlan(viewName, df.queryExecution.withCachedData, viewTable.queryExecution.withCachedData)
      // register rdd dataframe
      MaterializedViewCatalyst.getInstance().registerTableFromLogicalPlan(viewName, df.queryExecution.analyzed)
      df
    } else if (viewName.equals("line_item_agg_mv")) {
      val schema = new StructType()
        .add("L_RETURNFLAG", StringType)
        .add("L_LINESTATUS", StringType)
        .add("c", IntegerType)
      val rdd = CheckPointReflector.recover[InternalRow](spark, viewPath.toString)
      val df = CheckPointReflector.createDataFrame(spark, rdd, schema)
      val viewTable = spark.sql(viewCreateSql)
      // viewTable is the original logical plan AST tree, and df is another LogicalRDD
      // MaterializedViewCatalyst.getInstance().registerMaterializedViewFromCheckpointPath(viewName, viewPath.toString)
      // MaterializedViewCatalyst.getInstance().registerMaterializedViewFromCreateSql(viewName, viewCreateSql)
      MaterializedViewCatalyst.getInstance().registerMaterializedViewFromLogicalPlan(viewName, df.queryExecution.withCachedData, viewTable.queryExecution.withCachedData)
      // register rdd dataframe
      MaterializedViewCatalyst.getInstance().registerTableFromLogicalPlan(viewName, df.queryExecution.analyzed)
      df
    } else {
      throw new SparkException("invalid mv name:" + viewName)
    }
  }

  def createCkptMV(viewName: String, viewCreateSql: String): Unit = {
    val viewPath = new Path(spark.sparkContext.getCheckpointDir.get, viewName)
    val fs = viewPath.getFileSystem(spark.sessionState.newHadoopConf())
    if (fs.exists(viewPath)) {
      throw new SparkException("materialized view already exists:" + viewName)
    }

    logInfo("create checkpoint:" + viewPath)
    val viewTable = spark.sql(viewCreateSql)
    val types = attributionToString(viewTable.logicalPlan.output)
    MetadataClient.createMV(viewName, viewCreateSql, types)
    val df = spark.createDataFrame(viewTable.rdd, viewTable.schema)
    // df.persist(StorageLevel.DISK_ONLY)
    val conf: Properties = new Properties()
    conf.put("view_name", viewName)
    conf.put("view_sql", viewCreateSql)
    spark.sparkContext.setLocalProperties(conf)
    df.checkpoint() // [checkpointDir:viewName]
  }

  def createHiveMV(viewName: String, viewCreateSql: String): Unit = {
    logInfo("create checkpoint:" + viewName)
    val viewTable = spark.sql(viewCreateSql)
    val df = spark.createDataFrame(viewTable.rdd, viewTable.schema)
    // save to hive
    df.write.mode("overwrite").saveAsTable("mv." + viewName)
    val types = attributionToString(viewTable.logicalPlan.output)
    MetadataClient.createMV(viewName, viewCreateSql, types)
  }

  def toLogicalPlan(sql: String) = {
    val temp = spark.sql(sql).queryExecution.analyzed
    temp
  }

  def attributionToString(output: Seq[Attribute]): String = {
    val types: JsonArray = new JsonArray()
    output.foreach(attribute => {
      val column: JsonObject = new JsonObject()
      column.addProperty("name", attribute.name)
      column.addProperty("type", dataTypeToString(attribute.dataType))
      types.add(column)
    })
    types.toString
  }

  def dataTypeToString(dataType: DataType): String = {
    dataType match {
      case IntegerType => "Integer"
      case StringType => "String"
      case LongType => "Long"
      case _ => "InvalidType"
    }
  }

  def StringToDataType(str: String): DataType = {
    str match {
      case "Integer" => IntegerType
      case "String" => StringType
      case "Long" => LongType
      case _ => FloatType
    }
  }

//  def genSQL(lp: LogicalPlan) = {
//    val temp = new LogicalPlanSQL(lp, new BasicSQLDialect).toSQL
//    temp
//  }

//  def genPrettySQL(lp: LogicalPlan) = {
//    SQLUtils.format(genSQL(lp), JdbcConstants.HIVE)
//  }
}
