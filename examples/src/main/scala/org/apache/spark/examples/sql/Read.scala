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

object Read extends Logging {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL Read")
      // config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("hive.metastore.uris", "thrift://reins-PowerEdge-R740-0:9083")
      .config("spark.sql.warehouse.dir", "hdfs://reins-PowerEdge-R740-0:9000/zzt/data")
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()

    // query(spark)
    showTables(spark)
    // test(spark)
    // dropTable(spark)
  }

  def showTables(spark: SparkSession): Unit = {
    val sql =
      """
        |select l_returnflag, l_linestatus, SUM(l_quantity) as sum_qty, SUM(l_extendedprice) as sum_base_price
        |from LINEITEM
        |where L_SHIPDATE >= '1996-01-01' and L_SHIPDATE < '1997-01-01'
        |group by l_returnflag, l_linestatus
        |""".stripMargin
    val start = System.currentTimeMillis()
    spark.sql(sql).show()
    val end = System.currentTimeMillis()
    println(end - start)
    // println(spark.sql("select * from empts_large").count())
    // spark.sql("")
  }

  def dropTable(spark: SparkSession): Unit = {
    spark.sql("drop table empts_large").show()
  }

  def test(spark: SparkSession): Unit = {
    // spark.sql("select * from depts_hudi").show()
    spark.sql("select count(*) from nation").show() // 25
    spark.sql("select count(*) from part").show() // 200000
    spark.sql("select count(*) from supplier").show() // 10000
    spark.sql("select count(*) from partsupp").show() // 800000
    spark.sql("select count(*) from customer").show() // 150000
    spark.sql("select count(*) from orders").show() // 1500000
    spark.sql("select count(*) from lineitem").show() // 6001215
    spark.sql("select count(*) from region").show() // 5
  }

  def query(spark: SparkSession): Unit = {
    // 3
    spark.sql(
      """ select l_orderkey,
        |    sum(l_extendedprice * (1 - l_discount)) as revenue,
        |    o_orderdate,
        |    o_shippriority
        |from CUSTOMER, ORDERS, LINEITEM
        |where c_mktsegment = 'MACHINERY'
        |    and c_custkey = o_custkey
        |    and l_orderkey = o_orderkey
        |    and o_orderdate < date '1995-03-06'
        |    and l_shipdate > date '1995-03-06'
        |group by l_orderkey, o_orderdate, o_shippriority
        |order by revenue desc, o_orderdate;
        |""".stripMargin).show()

    // 4
    spark.sql(
      """select
        |    o_orderpriority,
        |    count(*) as order_count
        |from orders
        |where o_orderdate >= date '1993-10-01'
        |    and o_orderdate < date '1993-10-01' + interval '3' month
        |    and exists ( select * from lineitem
        |                where l_orderkey = o_orderkey
        |                and l_commitdate < l_receiptdate )
        |group by o_orderpriority
        |order by o_orderpriority;
        |""".stripMargin).show()
  }

}
