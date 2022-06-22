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

import org.apache.spark.examples.sql.tpch.TpcH.sink
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession

object SampleData extends Logging {
  private final val path = "/home/zzt/code/data/"

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Spark TPC-H")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("hive.metastore.uris", "thrift://reins-PowerEdge-R740-0:9083")
      .config("spark.sql.warehouse.dir", "hdfs://reins-PowerEdge-R740-0:9000/zzt/data")
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate()

    import spark.implicits._
    var data = spark.sparkContext.textFile(path + "depts.tbl")
      .map(_.split(','))
      .map(p => Depts(p(0).toInt, p(1).toString, p(2).toString))
      .toDF();
    data.show()
    // sink(data, "depts_large", "DEPTNO")
    // spark.sql("drop table depts_large").show()

    data = spark.sparkContext.textFile(path + "emps.tbl")
      .map(_.split(','))
      .map(p => Emps(p(0).toInt, p(1).toString, p(2).toString, p(3).toInt, p(4).toString, p(5).toInt, p(6).toInt))
      .toDF();
    data.show()
    sink(data, "emps_large", "EMPNO")
  }
}
