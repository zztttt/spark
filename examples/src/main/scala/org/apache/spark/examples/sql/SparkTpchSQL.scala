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
package org.apache.spark.examples.sql

import org.apache.spark.internal.Logging
import org.apache.spark.sql.{CheckPointReflector, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.mv.{MVOptimizRewrite, SchemaRegistry}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

object SparkTpchSQL extends Logging {
  private final val hdfs: String = "hdfs://reins-PowerEdge-R740-0:9000"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Debug")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("hive.metastore.uris", "thrift://reins-PowerEdge-R740-0:9083")
      .config("spark.sql.warehouse.dir", hdfs + "/zzt/data")
      .config("enable_materialized_view", value = true)
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate();

    spark.sparkContext.setCheckpointDir(hdfs + "/zzt")
    // spark.sparkContext.setMaterializedViewCheckpointDir(hdfs + "/zzt/checkpoint")

    runTpch(spark)
    // run6(spark);
    // loadCheckpoint(spark)
  }

  private def runTpch(spark: SparkSession): Unit = {
    val isCreate: Boolean = false
    val useMv: Boolean = false
    val mvSql =
      """
        |select L_RETURNFLAG, L_LINESTATUS, count(*) as c
        |from LINEITEM l
        |group by L_RETURNFLAG, L_LINESTATUS;
        |""".stripMargin
    val schemaRegistry: SchemaRegistry = new SchemaRegistry(spark);
    // schemaRegistry.createCkptMV("lineitem_agg_mv", mvSql)
    if (isCreate) schemaRegistry.createHiveMV("lineitem_agg__HiveMv", mvSql)

    // println(spark.catalog.tableExists("lineitem_mv"));
    // println(spark.sharedState.cacheManager.isEmpty)
    // spark.sql("select * from lineitem_mv").show();

    val original1: String =
      """
        |select
        |    L_RETURNFLAG,
        |    L_LINESTATUS,
        |    count(*) as c
        |from LINEITEM l
        |where L_RETURNFLAG = 'N'
        |group by L_RETURNFLAG, L_LINESTATUS;
        |""".stripMargin

//    val original2: String =
//      """
//        |select
//        |    L_RETURNFLAG,
//        |    L_LINESTATUS,
//        |    count(L_SHIPDATE)
//        |from LINEITEM l
//        |where L_RETURNFLAG != 'N'
//        |group by L_RETURNFLAG, L_LINESTATUS;
//        |""".stripMargin

    // original
    if (!isCreate && !useMv) {
      var startTime = System.currentTimeMillis()
      spark.sql(original1).show()
      var endTime = System.currentTimeMillis()
      val time1 = endTime - startTime
      logInfo("original time:" + String.valueOf(time1))
    } else {
      var startTime = System.currentTimeMillis()
      // val mv = schemaRegistry.loadCkptMV(spark, "lineitem_agg_mv")
      val mv = schemaRegistry.loadHiveMV(spark, "lineitem_agg__HiveMv")
      // mv.show()
      var endTime = System.currentTimeMillis()
      val time2 = endTime - startTime

      startTime = System.currentTimeMillis();
      val rewrite1 = MVOptimizRewrite.execute(schemaRegistry.toLogicalPlan(original1))
      endTime = System.currentTimeMillis()
      val time3 = endTime - startTime

      startTime = System.currentTimeMillis()
      spark.logicalPlanSql(rewrite1).show()
      endTime = System.currentTimeMillis()
      val time4 = endTime - startTime
      logInfo("load mv time:" + String.valueOf(time2))
      logInfo("rewrite time:" + String.valueOf(time3))
      logInfo("after rewritten time:" + String.valueOf(time4))
    }
  }

  private def run6(spark: SparkSession): Unit = {
    val schemaRegistry: SchemaRegistry = new SchemaRegistry(spark);
    schemaRegistry.loadCkptMV(spark, "lineitem_mv",
      """
        |select *
        |from lineitem
        |where l_shipdate >= '1996-01-01'
        |    and l_shipdate < '1997-01-01'
        |    and l_quantity < 25;
        |""".stripMargin
    )
    println(spark.catalog.tableExists("lineitem_mv"));
    // spark.sql("select * from lineitem_mv").show();
    val original: String =
      """
        |select *
        |from lineitem
        |where l_shipdate >= '1996-01-01'
        |    and l_shipdate < '1997-01-01'
        |    and l_discount between 0.04 and 0.06
        |    and l_quantity < 25;
        |""".stripMargin

    // original
    var startTime = System.currentTimeMillis()
    spark.sql(original).show()
    var endTime = System.currentTimeMillis()
    val time1 = endTime - startTime

    startTime = System.currentTimeMillis();
    val rewrite = MVOptimizRewrite.execute(schemaRegistry.toLogicalPlan(original))
    endTime = System.currentTimeMillis()
    val time2 = endTime - startTime

    startTime = System.currentTimeMillis();
    spark.logicalPlanSql(rewrite).show();
    endTime = System.currentTimeMillis()
    val time3 = endTime - startTime

    logInfo("original time:" + String.valueOf(time1))
    logInfo("rewrite time:" + String.valueOf(time2))
    logInfo("after rewritten time:" + String.valueOf(time3))
  }

  private def loadCheckpoint(spark: SparkSession): Unit = {
    val checkpointFilePath = hdfs + "/zzt/checkpoint_dir/lineitem_mv";
    val schema = new StructType()
      .add("L_RETURNFLAG", StringType)
      .add("L_LINESTATUS", StringType)
      .add("count(L_SHIPDATE)", IntegerType)

    val start = System.currentTimeMillis()
    val rdd = CheckPointReflector.recover[InternalRow](spark, checkpointFilePath)
    // rdd.foreach(x => println(x.toString))
    // println(rdd.count())
    val df = CheckPointReflector.createDataFrame(spark, rdd, schema)
    df.show()
    val end = System.currentTimeMillis()
    val time = end - start
    logInfo("load checkpoint:" + time)
  }
}
