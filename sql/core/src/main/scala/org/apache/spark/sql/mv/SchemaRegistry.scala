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
import org.apache.spark.sql.catalyst.{InternalRow, QueryPlanningTracker}
import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.{CheckPointReflector, DataFrame, SparkSession}
import org.apache.spark.sql.catalyst.plans.logical.{Aggregate, Filter, Join, LogicalPlan, Project, SubqueryAlias, Union}
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.execution.{LogicalRDD, SparkSqlParser}
import org.apache.spark.sql.mv.sqlGenerator.{BasicSQLDialect, LogicalPlanSQL}
import org.apache.spark.sql.types.{DataType, FloatType, IntegerType, LongType, StringType, StructType}

import java.util.Properties

class SchemaRegistry(_spark: SparkSession) extends Logging {
  val spark = _spark
  val parser: SparkSqlParser = new SparkSqlParser()

  def createTableInHudi(tableName: String, sql: String): MaterializedViewCatalyst = {
    spark.sql(sql)
    val logicalPlan = spark.table(tableName).queryExecution.analyzed match {
      case a@SubqueryAlias(_, child) => child
      case a@_ => a
    }
    MaterializedViewCatalyst.getInstance().registerTableFromLogicalPlan(tableName, logicalPlan)
  }

  def loadAll(spark: SparkSession): Unit = {
    val data: JsonArray = MetadataClient.loadAllMV().getAsJsonArray("data")
    data.forEach(name => {
//      logInfo("load mv:" + name)
      loadHiveMV(spark, name.getAsString)
    })
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
    //    logInfo("load mv:" + viewName)
    // 1. load metadata
    val start = System.currentTimeMillis()
    val metadata: JsonObject = MetadataClient.loadMV(viewName)
    if (metadata == null || metadata.entrySet().size() == 0) {
      logWarning("load empty")
      return null
    }
    logWarning("metadata:" + metadata.toString)
    val cur = System.currentTimeMillis()
    val createTime = metadata.get("createTime").getAsLong
    val ttl = metadata.get("ttl").getAsInt
    if (cur -  createTime > ttl) {
      logWarning("load success but expire")
      logWarning("gap:" + (cur - createTime) + " > " + ttl)
      return null
    }
    val viewCreateSql: String = metadata.get("createSql").getAsString
    logWarning("http:" + String.valueOf(System.currentTimeMillis() - start)) // 333
    //    val types = new Gson().fromJson(metadata.get("columnTypes").getAsString, classOf[JsonArray])
    //    var schema = new StructType()
    //    for(i <- 0 until types.size()) {
    //      val column = types.get(i).getAsJsonObject
    //      schema = schema.add(column.get("name").getAsString, StringToDataType(column.get("type").getAsString))
    //    }

    // 2. switch database and load dataframe
    // val viewTable = spark.sql(viewCreateSql)
//        val lp = spark.sessionState.sqlParser.parsePlan(viewCreateSql)
//        val analyzed = spark.sessionState.analyzer.executeAndCheck(lp, new QueryPlanningTracker())
    //    val analyzed = spark.sessionState.analyzer.execute(lp)
    val lp = parser.parsePlan(viewCreateSql)
    logWarning("parse:" + String.valueOf(System.currentTimeMillis() - start))
    val analyzed = spark.sessionState.analyzer.executeAndCheck(lp, new QueryPlanningTracker())
    logWarning("analyzed:" + String.valueOf(System.currentTimeMillis() - start)) // 7235

    val df = spark.read.table(viewName)
    df.queryExecution.analyzed
//    val dfAnalyzed = toLogicalPlan("select * from " + viewName)
    logWarning("read:" + String.valueOf(System.currentTimeMillis() - start)) // 8780
    //    df.cache()
    logWarning("cache:" + String.valueOf(System.currentTimeMillis() - start)) // 9051

    // register rdd dataframe
    MaterializedViewCatalyst.getInstance().registerMaterializedViewFromLogicalPlan(viewName, df.queryExecution.analyzed, analyzed)
    MaterializedViewCatalyst.getInstance().registerTableFromLogicalPlan(viewName, df.queryExecution.analyzed)
    logWarning("register:" + String.valueOf(System.currentTimeMillis() - start))
    df
  }

  def loadCandidate(spark: SparkSession, sql: String): Unit = {
    val candidate = MetadataClient.loadCandidate(sql)
    if (candidate.nonEmpty) {
      loadHiveMV(spark, candidate.head)
    } else {
      println("empty candidates.")
    }
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
//    logInfo("create hive mv:" + viewName)
    val viewTable = spark.sql(viewCreateSql)
    val df = spark.createDataFrame(viewTable.rdd, viewTable.schema)
    // save to hive
//    df.show()
    df.write.mode("overwrite").saveAsTable(viewName)
    val types = attributionToString(viewTable.logicalPlan.output)
    MetadataClient.createMV(viewName, viewCreateSql, types)
  }

  def toLogicalPlan(sql: String) = {
    val lp = parser.parsePlan(sql)
    spark.sessionState.analyzer.executeAndCheck(lp, new QueryPlanningTracker())
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

  def genSQL(lp: LogicalPlan) = {
    val temp = new LogicalPlanSQL(lp, new BasicSQLDialect).toSQL
    temp
  }

  def isStandard(plan: LogicalPlan): Boolean = {
    // scalastyle:off println
    println(plan)
    // scalastyle:on println
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

//  def genPrettySQL(lp: LogicalPlan) = {
//    SQLUtils.format(genSQL(lp), JdbcConstants.HIVE)
//  }
}
