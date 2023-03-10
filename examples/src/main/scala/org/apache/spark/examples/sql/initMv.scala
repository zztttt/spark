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
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.mv.SchemaRegistry

object initMv extends Logging {
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

    rewrite1(spark)
    rewrite2(spark)
    rewrite3(spark)
    rewrite4(spark)
    rewrite5(spark)
    rewrite6(spark)
    rewrite9(spark)
    rewrite10(spark)
    rewrite13(spark)
    rewrite16(spark)
    rewrite17(spark)
    rewrite18(spark)
  }

  private def rewrite1(spark: SparkSession): Unit = {
    val mv1: String =
      """
        |select L_RETURNFLAG, L_LINESTATUS,
        |       sum(L_QUANTITY) as qty,
        |       sum(L_EXTENDEDPRICE) as sum_base_price
        |from lineitem l
        |group by L_RETURNFLAG, L_LINESTATUS
        |""".stripMargin
    val mv2: String =
      """
        |select L_RETURNFLAG, L_LINESTATUS,
        |       count(*) as count_order
        |from lineitem l
        |group by L_RETURNFLAG, L_LINESTATUS
        |""".stripMargin
    create(mv1, "mv1_1", spark)
    create(mv2, "mv1_2", spark)
  }

  private def rewrite2(spark: SparkSession): Unit = {
    val mv1: String =
      """
        |select S_ACCTBAL, S_NAME, S_ADDRESS, S_PHONE, S_COMMENT
        |from PART, SUPPLIER, PARTSUPP, NATION, REGION
        |where P_PARTKEY = PS_PARTKEY
        |      and S_SUPPKEY = PS_SUPPKEY
        |      and S_NATIONKEY = N_NATIONKEY
        |      and N_REGIONKEY = R_REGIONKEY
        |""".stripMargin
    val mv2: String =
      """
        |select N_NAME, R_NAME
        |from PART, SUPPLIER, PARTSUPP, NATION, REGION
        |where P_PARTKEY = PS_PARTKEY
        |      and S_SUPPKEY = PS_SUPPKEY
        |      and S_NATIONKEY = N_NATIONKEY
        |      and N_REGIONKEY = R_REGIONKEY
        |""".stripMargin
    val mv3: String =
      """
        |select P_PARTKEY, P_MFGR, PS_SUPPLYCOST, P_SIZE, P_TYPE
        |from PART, SUPPLIER, PARTSUPP, NATION, REGION
        |where P_PARTKEY = PS_PARTKEY
        |      and S_SUPPKEY = PS_SUPPKEY
        |      and S_NATIONKEY = N_NATIONKEY
        |      and N_REGIONKEY = R_REGIONKEY
        |""".stripMargin
    create(mv1, "mv2_1", spark)
    create(mv2, "mv2_2", spark)
    create(mv3, "mv2_3", spark)
  }

  private def rewrite3(spark: SparkSession): Unit = {
    val mv1: String =
      """
        |select  L_ORDERKEY, sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue
        |from CUSTOMER, ORDERS, LINEITEM
        |where C_MKTSEGMENT = 'MACHINERY'
        |     and C_CUSTKEY = O_CUSTKEY
        |     and L_ORDERKEY = O_ORDERKEY
        |group by L_ORDERKEY, O_ORDERDATE, O_SHIPPRIORITY
        |""".stripMargin
    val mv2: String =
      """
        |select  sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue, O_ORDERDATE, O_SHIPPRIORITY
        |from CUSTOMER, ORDERS, LINEITEM
        |where C_MKTSEGMENT = 'MACHINERY'
        |     and C_CUSTKEY = O_CUSTKEY
        |     and L_ORDERKEY = O_ORDERKEY
        |group by L_ORDERKEY, O_ORDERDATE, O_SHIPPRIORITY
        |""".stripMargin
    create(mv1, "mv3_1", spark)
    create(mv2, "mv3_2", spark)
  }

  private def rewrite4(spark: SparkSession): Unit = {
    val mv: String =
      """
        |select O_ORDERPRIORITY, count(*) as order_count
        |from ORDERS
        |group by O_ORDERPRIORITY
        |""".stripMargin
  }

  private def rewrite5(spark: SparkSession): Unit = {
    val mv1: String =
      """
        |select N_NAME, sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue
        |from CUSTOMER, ORDERS, LINEITEM, SUPPLIER, NATION, REGION
        |where C_CUSTKEY = O_CUSTKEY
        |      and L_ORDERKEY = O_ORDERKEY
        |      and L_SUPPKEY = S_SUPPKEY
        |      and C_NATIONKEY = S_NATIONKEY
        |      and S_NATIONKEY = N_NATIONKEY
        |      and N_REGIONKEY = R_REGIONKEY
        |group by N_NAME, R_NAME
        |""".stripMargin
    val mv2: String =
      """
        |select R_NAME, sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as revenue
        |from CUSTOMER, ORDERS, LINEITEM, SUPPLIER, NATION, REGION
        |where C_CUSTKEY = O_CUSTKEY
        |      and L_ORDERKEY = O_ORDERKEY
        |      and L_SUPPKEY = S_SUPPKEY
        |      and C_NATIONKEY = S_NATIONKEY
        |      and S_NATIONKEY = N_NATIONKEY
        |      and N_REGIONKEY = R_REGIONKEY
        |group by N_NAME, R_NAME
        |""".stripMargin
    create(mv1, "mv5_1", spark)
    create(mv2, "mv5_2", spark)
  }

  private def rewrite6(spark: SparkSession): Unit = {
    val mv: String =
      """
        |select sum(L_EXTENDEDPRICE * L_DISCOUNT) as revenue
        |from LINEITEM
        |""".stripMargin
  }


  private def rewrite9(spark: SparkSession): Unit = {
    val mv1: String =
      """
        |select N_NAME, sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as amount
        |from PART, SUPPLIER, LINEITEM, PARTSUPP, ORDERS, NATION
        |where S_SUPPKEY = L_SUPPKEY
        |      and PS_SUPPKEY = L_SUPPKEY
        |      and PS_PARTKEY = L_PARTKEY
        |      and P_PARTKEY = L_PARTKEY
        |      and O_ORDERKEY = L_ORDERKEY
        |      and S_NATIONKEY = N_NATIONKEY
        |group by N_NAME, P_NAME
        |""".stripMargin
    val mv2: String =
      """
        |select P_NAME, sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as amount
        |from PART, SUPPLIER, LINEITEM, PARTSUPP, ORDERS, NATION
        |where S_SUPPKEY = L_SUPPKEY
        |      and PS_SUPPKEY = L_SUPPKEY
        |      and PS_PARTKEY = L_PARTKEY
        |      and P_PARTKEY = L_PARTKEY
        |      and O_ORDERKEY = L_ORDERKEY
        |      and S_NATIONKEY = N_NATIONKEY
        |group by N_NAME, P_NAME
        |""".stripMargin
    create(mv1, "mv9_1", spark)
    create(mv2, "mv9_2", spark)
  }

  private def rewrite10(spark: SparkSession): Unit = {
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
  }

  private def rewrite13(spark: SparkSession): Unit = {
    val mv: String =
      """
        |select C_CUSTKEY, count(*) as C_COUNT
        |from CUSTOMER left outer join ORDERS
        |on C_CUSTKEY = O_CUSTKEY
        |group by C_CUSTKEY
        |""".stripMargin
  }

  private def rewrite16(spark: SparkSession): Unit = {
    val mv: String =
      """
        |select P_BRAND, P_TYPE, P_SIZE,
        |       count(*) as supplier_cnt
        |from PARTSUPP, PART
        |where P_PARTKEY = PS_PARTKEY
        |group by P_BRAND, P_TYPE, P_SIZE
        |""".stripMargin
  }

  private def rewrite17(spark: SparkSession): Unit = {
    val mv: String =
      """
        |select P_BRAND, P_CONTAINER, sum(L_EXTENDEDPRICE / 7.0) as avg_year
        |from LINEITEM, PART
        |where P_PARTKEY = L_PARTKEY
        |group by P_BRAND, P_CONTAINER
        |""".stripMargin
  }

  private def rewrite18(spark: SparkSession): Unit = {
    val mv1: String =
      """
        |select C_NAME, C_CUSTKEY, sum(L_QUANTITY) as quantity
        |from CUSTOMER, ORDERS, LINEITEM
        |where C_CUSTKEY = O_CUSTKEY
        |      and O_ORDERKEY = L_ORDERKEY
        |group by C_NAME, C_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_TOTALPRICE
        |""".stripMargin
    val mv2: String =
      """
        |select O_ORDERKEY, O_ORDERDATE, O_TOTALPRICE, sum(L_QUANTITY) as quantity
        |from CUSTOMER, ORDERS, LINEITEM
        |where C_CUSTKEY = O_CUSTKEY
        |      and O_ORDERKEY = L_ORDERKEY
        |group by C_NAME, C_CUSTKEY, O_ORDERKEY, O_ORDERDATE, O_TOTALPRICE
        |""".stripMargin
    create(mv1, "mv18_1", spark)
    create(mv2, "mv18_2", spark)
  }

  private def create(mvSql: String, mvName: String, spark: SparkSession): Unit = {
    val schemaRegistry: SchemaRegistry = new SchemaRegistry(spark)
    schemaRegistry.createHiveMV(mvName, mvSql)
  }
}
