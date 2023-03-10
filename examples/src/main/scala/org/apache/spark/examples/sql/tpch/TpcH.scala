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
package org.apache.spark.examples.sql.tpch

import org.apache.hudi.DataSourceWriteOptions
import org.apache.hudi.DataSourceWriteOptions.{OPERATION_OPT_KEY, PARTITIONPATH_FIELD, RECORDKEY_FIELD}
import org.apache.hudi.QuickstartUtils.getQuickstartWriteConfigs
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object TpcH extends Logging {
  private final val path = "/Users/zzt/deploy/tpch_10G/"
  private final val host: String = "localhost"
  private final val hdfs: String = String.format("hdfs://%s:9000", host)
  private final val hiveMetastore = String.format("thrift://%s:9083", host)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark TPC-H")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
//      .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
//      .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
      .config("hive.metastore.uris", hiveMetastore)
      .config("spark.sql.warehouse.dir", hdfs + "/zzt/data")
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()
    // spark.catalog.setCurrentDatabase("MV")
    import spark.implicits._
    // nation
    //    val schema = new StructType()
    //      .add("N_NATIONKEY", IntegerType)
    //      .add("N_NAME", CharType(25))
    //      .add("N_REGIONKEY", IntegerType)
    //      .add("N_COMMENT", VarcharType(152))
    var data = spark.sparkContext.textFile(path + "nation.tbl")
      .map(_.split('|'))
      .map(p => Nation(p(0).toInt, p(1).toString, p(2).toInt, p(3).toString))
      .toDF();
    // data.write.mode(SaveMode.Overwrite).saveAsTable("test_hive")
    // data.show()
     sink(data, "nation", "N_NATIONKEY")
//    sinkDelta(data, "nation")

    // part
    data = spark.sparkContext.textFile(path + "part.tbl")
      .map(_.split('|'))
      .map(p => Part(p(0).toInt, p(1).toString, p(2).toString, p(3).toString, p(4).toString, p(5).toInt, p(6).toString, p(7).toDouble, p(8).toString))
      .toDF()
    // data.show()
     sink(data, "part", "P_PARTKEY")
//    sinkDelta(data, "part")

    // supplier
    data = spark.sparkContext.textFile(path + "supplier.tbl")
      .map(_.split('|'))
      .map(p => Supplier(p(0).toInt, p(1).toString, p(2).toString, p(3).toInt, p(4).toString, p(5).toDouble, p(6).toString))
      .toDF()
    // data.show()
     sink(data, "supplier", "S_SUPPKEY")
//    sinkDelta(data, "supplier")

    // partsupp
    data = spark.sparkContext.textFile(path + "partsupp.tbl")
      .map(_.split('|'))
      .map(p => Partsupp(p(0).toInt, p(1).toInt, p(2).toInt, p(3).toDouble, p(4).toString))
      .toDF()
    // data.show()
     sink(data, "partsupp", "PS_PARTKEY,PS_SUPPKEY")
//    sinkDelta(data, "partsupp")

    // customer
    data = spark.sparkContext.textFile(path + "customer.tbl")
      .map(_.split('|'))
      .map(p => Customer(p(0).toInt, p(1).toString, p(2).toString, p(3).toInt, p(4).toString, p(5).toDouble, p(6).toString, p(7).toString))
      .toDF()
    // data.show()
     sink(data, "customer", "C_CUSTKEY")
//    sinkDelta(data, "customer")

    // orders
    data = spark.sparkContext.textFile(path + "orders.tbl")
      .map(_.split('|'))
      .map(p => Orders(p(0).toInt, p(1).toInt, p(2).toString, p(3).toDouble, p(4).toString, p(5).toString, p(6).toString, p(7).toInt, p(8).toString))
      .toDF()
    // data.show()
    // data.createOrReplaceTempView("orders")
     sink(data, "orders", "O_ORDERKEY")
//    sinkDelta(data, "orders")

    // lineitem
    data = spark.sparkContext.textFile(path + "lineitem.tbl")
      .map(_.split('|'))
      .map(p => LineItem(p(0).toInt, p(1).toInt, p(2).toInt, p(3).toInt, p(4).toDouble, p(5).toDouble, p(6).toDouble, p(7).toDouble, p(8).toString, p(9).toString, p(10).toString, p(11).toString, p(12).toString, p(13).toString, p(14).toString, p(15).toString))
      .toDF()
    // data.show()
    // data.createOrReplaceTempView("lineItem")
    sink(data, "lineitem", "L_ORDERKEY,L_PARTKEY,L_SUPPKEY")
//    sinkDelta(data, "lineitem")
//    data.write.format("parquet").saveAsTable("lineitem_parquet")

    // region
    data = spark.sparkContext.textFile(path + "region.tbl")
      .map(_.split('|'))
      .map(p => Region(p(0).toInt, p(1).toString, p(2).toString))
      .toDF()
    // data.show()
     sink(data, "region", "R_REGIONKEY")
//    sinkDelta(data, "region")
  }

  def sink(data: DataFrame, tableName: String, recordKey: String, preCombine: String): Unit = {
    data.write.format("org.apache.hudi")
      .options(getQuickstartWriteConfigs)
      .option(HoodieWriteConfig.TABLE_NAME, tableName)
      .option(RECORDKEY_FIELD.key(), recordKey)
      .option(PARTITIONPATH_FIELD.key(), "")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, preCombine)
      .mode(SaveMode.Overwrite)
      .saveAsTable(tableName)
  }

  def sink(data: DataFrame, tableName: String, recordKey: String): Unit = {
    data.write.format("org.apache.hudi")
      .options(getQuickstartWriteConfigs)
      .option(HoodieWriteConfig.TABLE_NAME, tableName)
      .option(RECORDKEY_FIELD.key(), recordKey)
      .option(PARTITIONPATH_FIELD.key(), "")
      .option(DataSourceWriteOptions.KEYGENERATOR_CLASS_OPT_KEY, "org.apache.hudi.keygen.ComplexKeyGenerator")
      .option(OPERATION_OPT_KEY, "INSERT") // 指定了就不需要preCombine
      .option("hoodie.parquet.small.file.limit", "0")
      .option("hoodie.clustering.inline", "true")
      .option("hoodie.clustering.inline.max.commits", "4")
      .option("hoodie.clustering.plan.strategy.target.file.max.bytes", "1073741824")
      .option("hoodie.clustering.plan.strategy.small.file.limit", "629145600")
      .mode(SaveMode.Overwrite)
      .saveAsTable(tableName)
  }

  def sinkDelta(data: DataFrame, tableName: String): Unit = {
    data.write.format("delta").mode("overwrite").saveAsTable(tableName + "_delta")
  }
}
