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

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.StaticSQLConf

object SparkHudiSql {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Spark Debug")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("javax.jdo.option.ConnectionDriverName", "com.mysql.cj.jdbc.Driver")
      .config("javax.jdo.option.ConnectionURL", "jdbc:mysql://rm-uf67ktcrjo69g32viko.mysql.rds.aliyuncs.com:3306/metastore")
      .config("javax.jdo.option.ConnectionUserName", "zzt")
      .config("javax.jdo.option.ConnectionPassword", "Zzt19980924x")
      .config("hive.metastore.schema.verification", false)
      .config("hive.metastore.schema.verification.record.version", false)
      .config("datanucleus.autoCreateSchema", true)
      .config("datanucleus.autoCreateTables", true)
      .config("datanucleus.fixedDatastore", false)
      .config("datanucleus.readOnlyDatastore", false)
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate();
    // scalastyle:off println
    spark.sparkContext.getConf.getAll.foreach(println)
    val catalogType = spark.conf.get(StaticSQLConf.CATALOG_IMPLEMENTATION.key)
    println(catalogType);// should be hive
    // scalastyle:on println


    runCreateTable(spark);
  }
  private def runCreateTable(spark: SparkSession): Unit = {
    val showDatabases = "show databases";
    spark.sql(showDatabases).show();

    val useDefault = "use default";
    spark.sql(useDefault).show();

    val create = """create table if not exists table4 (id int, name string, int double)
                  using hudi location 'hdfs://10.0.0.180:9000/scala/table4'
                  options (type='mor', primaryKey='id')"""
    spark.sql(create);

    val insert = "insert into table4 select 1, 'zzt', 11";
    spark.sql(insert);

    val queryTable1 = "select * from default.table4";
    spark.sql(queryTable1).show();

    val showTable = "show tables";
    spark.sql(showTable).show();
  }
}
