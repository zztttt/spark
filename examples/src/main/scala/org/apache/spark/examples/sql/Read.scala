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
// import org.apache.spark.sql.catalyst.plans.logical.Join
// import org.apache.spark.sql.mv.SchemaRegistry

object Read extends Logging {
  private final val host: String = "localhost"
  private final val hdfs: String = String.format("hdfs://%s:9000", host)
  private final val hiveMetastore = String.format("thrift://%s:9083", host)
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .appName("Spark SQL Read")
      // config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      // .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("hive.metastore.uris", hiveMetastore)
      .config("spark.sql.warehouse.dir", hdfs + "/zzt/data")
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()

//    println(spark.catalog.currentDatabase)
//    spark.sessionState.catalog.listDatabases()
//      .foreach(f => println(f.toString))
//    spark.sessionState.catalog.listTables(spark.catalog.currentDatabase)
//      .foreach(f => println(f))

//    val lp = new SchemaRegistry(spark).toLogicalPlan("select * from mv1")
//    var ret = false;
//    lp transformDown { case a@Join(_, _, _, _, _) => ret = true; a}
//    println(ret)
//     query(spark)
     showTables(spark)
//     test(spark)
//     dropTable(spark)
  }

  def showTables(spark: SparkSession): Unit = {
    spark.sql("show tables").show()
  }

  def dropTable(spark: SparkSession): Unit = {
    spark.sql("drop table mv182").show()
  }

//  def test(spark: SparkSession): Unit = {
//    // spark.sql("select * from depts_hudi").show()
//    spark.sql("select count(*) from nation").show() // 25
//    spark.sql("select count(*) from part").show() // 200000
//    spark.sql("select count(*) from supplier").show() // 10000
//    spark.sql("select count(*) from partsupp").show() // 800000
//    spark.sql("select count(*) from customer").show() // 150000
//    spark.sql("select count(*) from orders").show() // 1500000
//    spark.sql("select count(*) from lineitem").show() // 6001215
//    spark.sql("select count(*) from region").show() // 5
//  }

  def query(spark: SparkSession): Unit = {
    // 3
    spark.sql(
      """
        |select C_CUSTKEY, count(O_ORDERKEY) as C_COUNT
        |from CUSTOMER left outer join ORDERS
        |on C_CUSTKEY = O_CUSTKEY and O_COMMENT not like'special%packages'
        |group by C_CUSTKEY
        |""".stripMargin
    ).show()

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
