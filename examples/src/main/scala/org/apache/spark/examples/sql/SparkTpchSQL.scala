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

import com.google.gson.Gson
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{CheckPointReflector, SparkSession}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.SparkSqlParser
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.mv.{MVOptimizRewrite, SchemaRegistry}
import org.apache.spark.sql.types.{IntegerType, StringType, StructType}

import java.io.ObjectOutputStream
import javax.json.JsonObject

object SparkTpchSQL extends Logging {
  private final val host: String = "localhost"
  private final val hdfs: String = String.format("hdfs://%s:9000", host)
  private final val hiveMetastore = String.format("thrift://%s:9083", host)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark Debug")
      .config("spark.serializer", "org.apache.spark.serializer.JavaSerializer")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("hive.metastore.uris", hiveMetastore)
      .config("spark.sql.warehouse.dir", hdfs + "/zzt/data")
      .config("hoodie.metadata.enable", value = false)
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate();

    spark.sparkContext.setLogLevel("WARN")
    // spark.sparkContext.setMaterializedViewCheckpointDir(hdfs + "/zzt/checkpoint")

//    rewrite1(spark)
//    rewrite2(spark)
//    rewrite3(spark)
//    rewrite4(spark)
//    rewrite5(spark)
//    rewrite6(spark)
//    rewrite9(spark)
//    rewrite10(spark)
//    rewrite13(spark)
//    rewrite16(spark)
//    rewrite17(spark)
    rewrite18(spark)
//    runBenchmark(spark)
  }

  private def rewrite1(spark: SparkSession): Unit = {
    val init: Boolean = false
    val rewrite: Boolean = true
    val mvName = "mv1"
    val mv: String =
      """
        |select L_RETURNFLAG, L_LINESTATUS,
        |       sum(L_QUANTITY) as qty,
        |       sum(L_EXTENDEDPRICE) as sum_base_price,
        |       count(*) as count_order
        |from lineitem l
        |group by L_RETURNFLAG, L_LINESTATUS
        |""".stripMargin
    val q: String =
      """
        |select L_RETURNFLAG, L_LINESTATUS,
        |       sum(L_QUANTITY) as qty,
        |       sum(L_EXTENDEDPRICE) as sum_base_price,
        |       count(*) as count_order
        |from lineitem l
        |where L_LINESTATUS <> 'O'
        |group by L_RETURNFLAG, L_LINESTATUS
        |""".stripMargin
    rewriteTemplate(init, rewrite, mv, mvName, q, spark)
  }

  private def rewrite2(spark: SparkSession): Unit = {
    val init: Boolean = false
    val rewrite: Boolean = true
    val mvName = "mv2"
    val mv: String =
      """
        |select S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT, R_NAME, PS_SUPPLYCOST, P_SIZE, P_TYPE
        |from PART, SUPPLIER, PARTSUPP, NATION, REGION
        |where P_PARTKEY = PS_PARTKEY
        |      and S_SUPPKEY = PS_SUPPKEY
        |      and S_NATIONKEY = N_NATIONKEY
        |      and N_REGIONKEY = R_REGIONKEY
        |""".stripMargin
//    val q: String =
//      """
//        |select S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT, R_NAME, PS_SUPPLYCOST, P_SIZE, P_TYPE
//        |from PART, SUPPLIER, PARTSUPP, NATION, REGION
//        |where P_PARTKEY = PS_PARTKEY
//        |      and S_SUPPKEY = PS_SUPPKEY
//        |      and S_NATIONKEY = N_NATIONKEY
//        |      and N_REGIONKEY = R_REGIONKEY
//        |      and R_NAME = 'AMERICA'
//        |      and PS_SUPPLYCOST < '500.0'
//        |      and P_SIZE = 30
//        |      and P_TYPE like '%SMALL%'
//        |""".stripMargin
    val q =
      """
        |select S_ACCTBAL, S_NAME, N_NAME, P_PARTKEY, P_MFGR, S_ADDRESS, S_PHONE, S_COMMENT, R_NAME, PS_SUPPLYCOST, P_SIZE, P_TYPE
        |from PART, SUPPLIER, PARTSUPP, NATION, REGION
        |where P_PARTKEY = PS_PARTKEY
        |and S_SUPPKEY = PS_SUPPKEY
        |and S_NATIONKEY = N_NATIONKEY
        |and N_REGIONKEY = R_REGIONKEY
        |and R_NAME = 'AMERICA'
        |and PS_SUPPLYCOST < '302.0'
        |and P_SIZE = '36'
        |""".stripMargin
    rewriteTemplate(init, rewrite, mv, mvName, q, spark)
  }

  private def rewrite3(spark: SparkSession): Unit = {
    val init: Boolean = false
    val rewrite: Boolean = true
    val mvName = "mv3"
    val mv: String =
      """
        |select  L_ORDERKEY, sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue, O_ORDERDATE, O_SHIPPRIORITY
        |from CUSTOMER, ORDERS, LINEITEM
        |where C_MKTSEGMENT = 'MACHINERY'
        |     and C_CUSTKEY = O_CUSTKEY
        |     and L_ORDERKEY = O_ORDERKEY
        |group by L_ORDERKEY, O_ORDERDATE, O_SHIPPRIORITY
        |""".stripMargin
//    val q3: String =
//      """
//        |select  L_ORDERKEY, sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue, O_ORDERDATE, O_SHIPPRIORITY
//        |from CUSTOMER, ORDERS, LINEITEM
//        |where C_MKTSEGMENT = 'MACHINERY'
//        |     and C_CUSTKEY = O_CUSTKEY
//        |     and L_ORDERKEY = O_ORDERKEY
//        |     and O_ORDERDATE < '1995-03-06'
//        |group by L_ORDERKEY, O_ORDERDATE, O_SHIPPRIORITY
//        |""".stripMargin
    val q3 =
      """
        |select  L_ORDERKEY, sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue, O_ORDERDATE, O_SHIPPRIORITY
        |from CUSTOMER, ORDERS, LINEITEM
        |where C_MKTSEGMENT = 'MACHINERY'
        |and C_CUSTKEY = O_CUSTKEY
        |and L_ORDERKEY = O_ORDERKEY
        |and O_ORDERDATE < '1995-03-06'
        |group by L_ORDERKEY, O_ORDERDATE, O_SHIPPRIORITY
        |""".stripMargin
    rewriteTemplate(init, rewrite, mv, mvName, q3, spark)
  }

  private def rewrite4(spark: SparkSession): Unit = {
    val init: Boolean = false
    val rewrite: Boolean = true
    val mvName = "mv4"
    val mv: String =
      """
        |select O_ORDERPRIORITY, count(*) as order_count
        |from ORDERS
        |group by O_ORDERPRIORITY
        |""".stripMargin
    val q: String =
      """
        |select O_ORDERPRIORITY, count(*) as order_count
        |from ORDERS
        |where O_ORDERPRIORITY = '1-URGENT'
        |group by O_ORDERPRIORITY
        |""".stripMargin
    rewriteTemplate(init, rewrite, mv, mvName, q, spark)
  }

  private def rewrite5(spark: SparkSession): Unit = {
    val init: Boolean = false
    val rewrite: Boolean = true
    val mvName = "mv5"
    val mv: String =
      """
        |select N_NAME, R_NAME, sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue
        |from CUSTOMER, ORDERS, LINEITEM, SUPPLIER, NATION, REGION
        |where C_CUSTKEY = O_CUSTKEY
        |      and L_ORDERKEY = O_ORDERKEY
        |      and L_SUPPKEY = S_SUPPKEY
        |      and C_NATIONKEY = S_NATIONKEY
        |      and S_NATIONKEY = N_NATIONKEY
        |      and N_REGIONKEY = R_REGIONKEY
        |group by N_NAME, R_NAME
        |""".stripMargin
    val q: String =
      """
        |select N_NAME, R_NAME, sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue
        |from CUSTOMER, ORDERS, LINEITEM, SUPPLIER, NATION, REGION
        |where C_CUSTKEY = O_CUSTKEY
        |      and L_ORDERKEY = O_ORDERKEY
        |      and L_SUPPKEY = S_SUPPKEY
        |      and C_NATIONKEY = S_NATIONKEY
        |      and S_NATIONKEY = N_NATIONKEY
        |      and N_REGIONKEY = R_REGIONKEY
        |      and R_NAME = 'AMERICA'
        |group by N_NAME, R_NAME
        |""".stripMargin
    rewriteTemplate(init, rewrite, mv, mvName, q, spark)
  }

  private def rewrite6(spark: SparkSession): Unit = {
    val init: Boolean = false
    val rewrite: Boolean = true
    val mvName = "mv6"
    val mv: String =
      """
        |select sum(L_EXTENDEDPRICE * L_DISCOUNT) as revenue
        |from LINEITEM
        |""".stripMargin
    val q: String =
      """
        |select sum(L_EXTENDEDPRICE * L_DISCOUNT) as revenue
        |from LINEITEM
        |""".stripMargin
    rewriteTemplate(init, rewrite, mv, mvName, q, spark)
  }

//  private def rewrite7(spark: SparkSession): Unit = {
//    val init: Boolean = false
//    val rewrite: Boolean = true
//    val mvName = "mv7"
//    val mv: String =
//      """
//        |select n1.N_NAME, n2.N_NAME, L_EXTENDEDPRICE * (1 - L_DISCOUNT) as volumn
//        |from SUPPLIER, LINEITEM, ORDERS, CUSTOMER, NATION n1, NATION n2
//        |where S_SUPPKEY = L_SUPPKEY
//        |      and O_ORDERKEY = L_ORDERKEY
//        |      and C_CUSTKEY = O_CUSTKEY
//        |      and S_NATIONKEY = n1.N_NATIONKEY
//        |      and C_NATIONKEY = n2.N_NATIONKEY
//        |""".stripMargin
//    val q: String =
//      """
//        |select n1.N_NAME, n2.N_NAME, L_EXTENDEDPRICE * (1 - L_DISCOUNT) as volumn
//        |from SUPPLIER, LINEITEM, ORDERS, CUSTOMER, NATION n1, NATION n2
//        |where S_SUPPKEY = L_SUPPKEY
//        |      and O_ORDERKEY = L_ORDERKEY
//        |      and C_CUSTKEY = O_CUSTKEY
//        |      and S_NATIONKEY = n1.N_NATIONKEY
//        |      and C_NATIONKEY = n2.N_NATIONKEY
//        |      and n1.N_NAME = 'FRANCE' and n2.N_NAME = 'IRAQ'
//        |""".stripMargin
//    rewriteTemplate(init, rewrite, mv, mvName, q, spark)
//  }

  private def rewrite9(spark: SparkSession): Unit = {
    val init: Boolean = false
    val rewrite: Boolean = true
    val mvName = "mv9"
    val mv: String =
      """
        |select N_NAME, P_NAME, sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as amount
        |from PART, SUPPLIER, LINEITEM, PARTSUPP, ORDERS, NATION
        |where S_SUPPKEY = L_SUPPKEY
        |      and PS_SUPPKEY = L_SUPPKEY
        |      and PS_PARTKEY = L_PARTKEY
        |      and P_PARTKEY = L_PARTKEY
        |      and O_ORDERKEY = L_ORDERKEY
        |      and S_NATIONKEY = N_NATIONKEY
        |group by N_NAME, P_NAME
        |""".stripMargin
    val q: String =
      """
        |select N_NAME, P_NAME, sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as amount
        |from PART, SUPPLIER, LINEITEM, PARTSUPP, ORDERS, NATION
        |where S_SUPPKEY = L_SUPPKEY
        |      and PS_SUPPKEY = L_SUPPKEY
        |      and PS_PARTKEY = L_PARTKEY
        |      and P_PARTKEY = L_PARTKEY
        |      and O_ORDERKEY = L_ORDERKEY
        |      and S_NATIONKEY = N_NATIONKEY
        |      and P_NAME like '%blue%'
        |group by N_NAME, P_NAME
        |""".stripMargin
    rewriteTemplate(init, rewrite, mv, mvName, q, spark)
  }

  private def rewrite10(spark: SparkSession): Unit = {
    val init: Boolean = false
    val rewrite: Boolean = true
    val mvName = "mv10"
    val mv: String =
      """
        |select C_CUSTKEY, C_NAME,
        |       sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue,
        |       C_ACCTBAL, N_NAME, C_ADDRESS, C_PHONE, C_COMMENT, L_RETURNFLAG
        |from CUSTOMER, ORDERS, LINEITEM, NATION
        |where C_CUSTKEY = O_CUSTKEY
        |      and L_ORDERKEY = O_ORDERKEY
        |      and C_NATIONKEY = N_NATIONKEY
        |group by C_CUSTKEY, C_NAME, C_ACCTBAL, C_PHONE, N_NAME, C_ADDRESS, C_COMMENT, L_RETURNFLAG
        |""".stripMargin
    val q4: String =
      """
        |select C_CUSTKEY, C_NAME,
        |       sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue,
        |       C_ACCTBAL, N_NAME, C_ADDRESS, C_PHONE, C_COMMENT, L_RETURNFLAG
        |from CUSTOMER, ORDERS, LINEITEM, NATION
        |where C_CUSTKEY = O_CUSTKEY
        |      and L_ORDERKEY = O_ORDERKEY
        |      and C_NATIONKEY = N_NATIONKEY
        |      and L_RETURNFLAG = 'R'
        |group by C_CUSTKEY, C_NAME, C_ACCTBAL, C_PHONE, N_NAME, C_ADDRESS, C_COMMENT, L_RETURNFLAG
        |""".stripMargin
    rewriteTemplate(init, rewrite, mv, mvName, q4, spark)
  }

  private def rewrite13(spark: SparkSession): Unit = {
    val init: Boolean = false
    val rewrite: Boolean = true
    val mvName = "mv13"
    val mv: String =
      """
        |select C_CUSTKEY, count(*) as C_COUNT
        |from CUSTOMER left outer join ORDERS
        |on C_CUSTKEY = O_CUSTKEY
        |group by C_CUSTKEY
        |""".stripMargin
    val q: String =
      """
        |select OUT_COUNT, count(*) as custdist
        |from (select C_CUSTKEY, count(*) as C_COUNT
        |      from CUSTOMER left outer join ORDERS
        |      on C_CUSTKEY = O_CUSTKEY
        |      group by C_CUSTKEY) as c_orders (C_CUSTKEY, OUT_COUNT)
        |group by OUT_COUNT
        |""".stripMargin
    rewriteTemplate(init, rewrite, mv, mvName, q, spark)
  }

  private def rewrite16(spark: SparkSession): Unit = {
    val init: Boolean = false
    val rewrite: Boolean = true
    val mvName = "mv16"
    val mv: String =
      """
        |select P_BRAND, P_TYPE, P_SIZE,
        |       count(*) as supplier_cnt
        |from PARTSUPP, PART
        |where P_PARTKEY = PS_PARTKEY
        |group by P_BRAND, P_TYPE, P_SIZE
        |""".stripMargin
    val q: String =
      """
        |select P_BRAND, P_TYPE, P_SIZE,
        |       count(*) as supplier_cnt
        |from PARTSUPP, PART
        |where P_PARTKEY = PS_PARTKEY
        |      and P_BRAND <> 'Brand#13'
        |      and P_SIZE in (1, 2, 3, 4, 5, 6, 7, 8)
        |group by P_BRAND, P_TYPE, P_SIZE
        |""".stripMargin
    rewriteTemplate(init, rewrite, mv, mvName, q, spark)
  }

  private def rewrite17(spark: SparkSession): Unit = {
    val init: Boolean = false
    val rewrite: Boolean = true
    val mvName = "mv17"
    val mv: String =
      """
        |select P_BRAND, P_CONTAINER, sum(L_EXTENDEDPRICE / 7.0) as avg_year
        |from LINEITEM, PART
        |where P_PARTKEY = L_PARTKEY
        |group by P_BRAND, P_CONTAINER
        |""".stripMargin
    val q: String =
      """
        |select P_BRAND, P_CONTAINER, sum(L_EXTENDEDPRICE / 7.0) as avg_year
        |from LINEITEM, PART
        |where P_PARTKEY = L_PARTKEY
        |      and P_BRAND = 'Brand#13'
        |      and P_CONTAINER = 'SM PKG'
        |group by P_BRAND, P_CONTAINER
        |""".stripMargin
    rewriteTemplate(init, rewrite, mv, mvName, q, spark)
  }

  private def rewrite18(spark: SparkSession): Unit = {
    val init: Boolean = false
    val rewrite: Boolean = true
    val mvName = "mv18"
    val mv: String =
      """
        |select C_NAME, C_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_TOTALPRICE, sum(L_QUANTITY) as quantity
        |from CUSTOMER, ORDERS, LINEITEM
        |where C_CUSTKEY = O_CUSTKEY
        |      and O_ORDERKEY = L_ORDERKEY
        |group by C_NAME, C_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_TOTALPRICE
        |""".stripMargin
    val q: String =
      """
        |select C_NAME, C_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_TOTALPRICE, sum(L_QUANTITY) as quantity
        |from CUSTOMER, ORDERS, LINEITEM
        |where O_ORDERKEY in (43609317, 28077922, 56497062)
        |      and C_CUSTKEY = O_CUSTKEY
        |      and O_ORDERKEY = L_ORDERKEY
        |group by C_NAME, C_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_TOTALPRICE
        |""".stripMargin
    rewriteTemplate(init, rewrite, mv, mvName, q, spark)
  }

  private def rewriteTemplate(init: Boolean, enableRewrite: Boolean,
                              mvSql: String, mvName: String,
                              query: String, spark: SparkSession): Unit = {
    val schemaRegistry: SchemaRegistry = new SchemaRegistry(spark);
    if (init) {
      val startTime = System.currentTimeMillis()
      schemaRegistry.createHiveMV(mvName, mvSql)
      val endTime = System.currentTimeMillis()
      log.warn("init time:" + (endTime - startTime))
    }
    if (enableRewrite) {
      var startTime = System.currentTimeMillis()
//      schemaRegistry.loadAll(spark)
//      schemaRegistry.loadHiveMV(spark, mvName)
      schemaRegistry.loadCandidate(spark, query)
      var endTime = System.currentTimeMillis()
      val time2 = endTime - startTime

      startTime = System.currentTimeMillis()
      val lp = schemaRegistry.toLogicalPlan(query)
      val rewrite = MVOptimizRewrite.execute(lp)
      endTime = System.currentTimeMillis()
      val time3 = endTime - startTime

      startTime = System.currentTimeMillis()
      if (schemaRegistry.isStandard(rewrite)) {
        spark.logicalPlanSql(rewrite).show()
      } else {
        if (rewrite == lp) {
          // rewrite fail
          spark.logicalPlanSql(rewrite).show()
        } else {
          // success
          val newSql = schemaRegistry.genSQL(rewrite)
          System.out.println(newSql)
          spark.sql(newSql).show()
        }

      }
      endTime = System.currentTimeMillis()
      val time4 = endTime - startTime
      logWarning("load mv time:" + String.valueOf(time2))
      logWarning("rewrite time:" + String.valueOf(time3))
      logWarning("after rewritten time:" + String.valueOf(time4))
      startTime = System.currentTimeMillis()
//      schemaRegistry.loadHiveMV(spark, "mv1")
//      schemaRegistry.loadHiveMV(spark, "mv2")
//      schemaRegistry.loadHiveMV(spark, "mv4")
//      schemaRegistry.loadHiveMV(spark, "mv5")
//      schemaRegistry.loadHiveMV(spark, "mv6")
//      schemaRegistry.loadHiveMV(spark, "mv9")
//      schemaRegistry.loadHiveMV(spark, "mv10")
//      schemaRegistry.loadHiveMV(spark, "mv13")
//      schemaRegistry.loadHiveMV(spark, "mv16")
//      schemaRegistry.loadHiveMV(spark, "mv17")
//      schemaRegistry.loadHiveMV(spark, "mv18")
      endTime = System.currentTimeMillis()
      logWarning("other time:" + String.valueOf(endTime - startTime))
    } else {
      var startTime = System.currentTimeMillis()
      val tmp = spark.sql(query)
      tmp.show()
      var endTime = System.currentTimeMillis()
      val time1 = endTime - startTime
      logWarning("baseline time:" + String.valueOf(time1))
    }
  }

//  private def runBenchmark(spark: SparkSession): Unit = {
//    val q1: String =
//      """
//        |select L_RETURNFLAG, L_LINESTATUS, sum(L_QUANTITY) as qty
//        |from LINEITEM l
//        |where L_SHIPDATE <= '1998-12-01'
//        |group by L_RETURNFLAG, L_LINESTATUS
//        |order by L_RETURNFLAG, L_LINESTATUS
//        |""".stripMargin
//    spark.sql(q1).show()
//
//
//    val q3: String =
//      """
//        |select  L_ORDERKEY, sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue, O_ORDERDATE, O_SHIPPRIORITY
//        |from CUSTOMER, ORDERS, LINEITEM
//        |where C_MKTSEGMENT = 'MACHINERY'
//        |     and C_CUSTKEY = O_CUSTKEY
//        |     and L_ORDERKEY = O_ORDERKEY
//        |     and O_ORDERDATE < '1995-03-06'
//        |     and L_SHIPDATE > '1995-03-06'
//        |group by L_ORDERKEY, O_ORDERDATE, O_SHIPPRIORITY
//        |order by REVENUE desc, O_ORDERDATE
//        |""".stripMargin
//
//    val q4: String =
//      """
//        |select O_ORDERPRIORITY, count(*) as order_count
//        |from ORDERS
//        |where O_ORDERDATE >= '1993-10-01'
//        |      and O_ORDERDATE < '1994-01-01'
//        |      and exists (select * from LINEITEM
//        |                 where L_ORDERKEY = O_ORDERKEY
//        |                       AND L_COMMITDATE < L_RECEIPTDATE)
//        |group by O_ORDERPRIORITY
//        |order by O_ORDERPRIORITY
//        |""".stripMargin
//
//    val q6: String =
//      """
//        |select sum(L_EXTENDEDPRICE * L_DISCOUNT) as revenue
//        |from LINEITEM
//        |where L_SHIPDATE >= '1996-01-01'
//        |      and L_SHIPDATE < '1997-01-01'
//        |      and L_DISCOUNT between 0.04 and 0.06
//        |      and L_QUANTITY < 25
//        |""".stripMargin
//
//    val q10: String =
//      """
//        |select C_CUSTKEY, C_NAME,
//        |       sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue,
//        |       C_ACCTBAL, N_NAME, C_ADDRESS, C_PHONE, C_COMMENT
//        |from CUSTOMER, ORDERS, LINEITEM, NATION
//        |where C_CUSTKEY = O_CUSTKEY
//        |      and L_ORDERKEY = O_ORDERKEY
//        |      and O_ORDERDATE >= '1993-01-01'
//        |      and O_ORDERDATE <= '1994-01-01'
//        |      and L_RETURNFLAG = 'R'
//        |      and C_NATIONKEY = N_NATIONKEY
//        |group by C_CUSTKEY, C_NAME, C_ACCTBAL, C_PHONE, N_NAME, C_ADDRESS, C_COMMENT
//        |order by revenue desc
//        |""".stripMargin
//
//    val q11: String =
//      """
//        |select PS_PARTKEY, sum(PS_SUPPLYCOST * PS_AVAILQTY) as value
//        |from PARTSUPP, SUPPLIER, NATION
//        |where PS_SUPPKEY = S_SUPPKEY
//        |      and S_NATIONKEY = N_NATIONKEY
//        |      and N_NAME = 'FRANCE'
//        |group by PS_PARTKEY
//        |having sum(PS_SUPPLYCOST * PS_AVAILQTY) >
//        |       (select sum(PS_SUPPLYCOST * PS_AVAILQTY) * 0.0001
//        |       from PARTSUPP, SUPPLIER, NATION
//        |       where PS_SUPPKEY = S_SUPPKEY
//        |       and S_NATIONKEY = N_NATIONKEY
//        |       and N_NAME = 'FRANCE')
//        |order by value desc
//        |""".stripMargin

//    val q12: String =
//      """
//        |select L_SHIPMODE,
//        |       sum(case when O_ORDERPRIORITY = '1-URGENT' or O_ORDERPRIORITY = '2-HIGH' then 1 else 0 end) as high_line_count,
//        |       sum(case when O_ORDERPRIORITY <> '1-URGENT' and O_ORDERPRIORITY <> '2-HIGH' then 1 else 0 end) as low_line_count
//        |from orders, lineitem
//        |where O_ORDERKEY = L_ORDERKEY
//        |      and L_SHIPMODE in ('AIR', 'SHIP')
//        |      and L_COMMITDATE < L_RECEIPTDATE
//        |      and L_RECEIPTDATE >= '1993-01-01'
//        |      and L_RECEIPTDATE < '1994-01-01'
//        |group by L_SHIPMODE
//        |order by L_SHIPMODE
//        |""".stripMargin

//
//    val q13: String =
//      """
//        |select C_COUNT, count(*) as custdist
//        |from (select C_CUSTKEY, count(O_ORDERKEY)
//        |      from CUSTOMER left outer join ORDERS
//        |      on C_CUSTKEY = O_CUSTKEY and O_COMMENT not like'special%packages'
//        |      group by C_CUSTKEY) as c_orders (C_CUSTKEY, C_COUNT)
//        |group by C_COUNT
//        |order by custdist desc, C_COUNT desc
//        |""".stripMargin

//
//    val q14: String =
//      """
//        |select sum(case when P_TYPE like 'PROMO%' then L_EXTENDEDPRICE * (1 - L_DISCOUNT) else 0 end)
//        |       / sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as promo_revenue
//        |from LINEITEM, part
//        |where L_PARTKEY = P_PARTKEY
//        |      and L_SHIPDATE >= '1993-01-01'
//        |      and L_SHIPDATE < '1994-01-01'
//        |""".stripMargin

//
//    val q16: String =
//      """
//        |select P_BRAND, P_TYPE, P_SIZE,
//        |       count(distinct PS_SUPPKEY) as supplier_cnt
//        |from PARTSUPP, PART
//        |where P_PARTKEY = PS_PARTKEY
//        |      and P_BRAND <> 'Brand#13'
//        |      and P_SIZE in (1, 2, 3, 4, 5, 6, 7, 8)
//        |      and PS_SUPPKEY not in (select S_SUPPKEY from SUPPLIER where S_COMMENT like 'Customer%Complaints')
//        |group by P_BRAND, P_TYPE, P_SIZE
//        |order by supplier_cnt desc, P_BRAND, P_TYPE, P_SIZE
//        |""".stripMargin

//
//    val q17: String =
//      """
//        |select sum(L_EXTENDEDPRICE) / 7.0 as avg_year
//        |from LINEITEM, PART
//        |where P_PARTKEY = L_PARTKEY
//        |      and P_BRAND = 'Brand#13'
//        |      and P_CONTAINER = 'SM PKG'
//        |      and L_QUANTITY < (select 0.2 * avg(L_QUANTITY) from LINEITEM where L_PARTKEY = P_PARTKEY)
//        |""".stripMargin

//
//    val q18: String =
//      """
//        |select C_NAME, C_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_TOTALPRICE, sum(L_QUANTITY)
//        |from CUSTOMER, ORDERS, LINEITEM
//        |where O_ORDERKEY in (select L_ORDERKEY from LINEITEM group by L_ORDERKEY having sum(L_QUANTITY) > 312)
//        |      and C_CUSTKEY = O_CUSTKEY
//        |      and O_ORDERKEY = L_ORDERKEY
//        |group by C_NAME, C_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_TOTALPRICE
//        |order by O_TOTALPRICE desc, O_ORDERDATE
//        |""".stripMargin

//  }

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

  private def test(spark: SparkSession): Unit = {
    val sql =
      """
        |select L_RETURNFLAG, L_LINESTATUS,
        |       sum(L_QUANTITY) as qty,
        |       sum(L_EXTENDEDPRICE) as sum_base_price,
        |       count(*) as coun_order
        |from lineitem l
        |where L_SHIPDATE <= '1998-12-01' and L_LINESTATUS <> 'O'
        |group by L_RETURNFLAG, L_LINESTATUS
        |""".stripMargin
    val start = System.currentTimeMillis()
    val parser = new SparkSqlParser()
    val lp = parser.parsePlan(sql)
    System.out.println("parse:" + String.valueOf(System.currentTimeMillis() - start))
    val qe = spark.sessionState.executePlan(lp)
    System.out.println(qe.tracker.phases)
    val analyzed = qe.analyzed
    qe.tracker.phases.foreach(f => System.out.println(f))
    qe.tracker.rules.foreach(f => System.out.println(f))
    System.out.println("analyze:" + String.valueOf(System.currentTimeMillis() - start))
    spark.logicalPlanSql(analyzed).show()
    val fileSystem = FileSystem.get(spark.sessionState.newHadoopConfWithOptions(Map("fs.defaultFS" -> hdfs)))
    val path = new Path("/test/lp")
    System.out.println("print:" + String.valueOf(System.currentTimeMillis() - start))
    val out: ObjectOutputStream = new ObjectOutputStream(fileSystem.create(path))
    analyzed.transformDown {
      case a@LogicalRelation(_, _, _, _) =>
        //        logWarning(a.toString)
        a
    }
    val gson = new Gson()
    analyzed.collectLeaves().foreach(f => {
      f.toJSON
      val json = gson.toJson(f)
      val lp = gson.fromJson(json, classOf[JsonObject])
      System.out.println(lp)
    })
    System.out.println(1)
  }

  private def testMv(spark: SparkSession): Unit = {
    val schemaRegistry: SchemaRegistry = new SchemaRegistry(spark);
    schemaRegistry.loadHiveMV(spark, "mv1")
  }
}
