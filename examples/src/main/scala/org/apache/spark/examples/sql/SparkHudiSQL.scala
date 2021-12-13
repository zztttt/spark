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

object SparkHudiSQL {
  def main(args: Array[String]): Unit = {
    val mysqlUrl = "rm-uf67ktcrjo69g32viko.mysql.rds.aliyuncs.com:3306/metastore";
    val mysqlPlaceHolder = "jdbc:mysql://%s?createDatabaseIfNotExist=true";
    val spark = SparkSession.builder()
      .appName("Spark Debug")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("javax.jdo.option.ConnectionURL", mysqlPlaceHolder.format(mysqlUrl))
      .config("javax.jdo.option.ConnectionDriverName", "com.mysql.cj.jdbc.Driver")
      .config("javax.jdo.option.ConnectionUserName", "zzt")
      .config("javax.jdo.option.ConnectionPassword", "Zzt19980924x")
      .config("spark.hadoop.datanucleus.autoCreateSchema", true)
      .config("spark.hadoop.datanucleus.autoCreateTables", true)
      .config("spark.hadoop.datanucleus.fixedDatastore", false)
      .config("spark.hadoop.datanucleus.readOnlyDatastore", false)
      .config("spark.hadoop.datanucleus.autoStartMechanism", "SchemaTable")
      .config("spark.hadoop.datanucleus.autoStartMechanism", "SchemaTable")
      .config("spark.hadoop.hive.metastore.schema.verification", false)
      .config("spark.hadoop.hive.metastore.schema.verification.record.version", false)
      .enableHiveSupport()
      .master("local[*]")
      .getOrCreate();
    runCreateTable(spark);
  }
  private def runCreateTable(spark: SparkSession): Unit = {
    val showDatabases = "show databases";
    spark.sql(showDatabases).show();

    val useDefault = "use default";
    spark.sql(useDefault).show();

    val tableName = "hudi_mor_tbl8";
    val createPlaceHolder = """create table %s (id int, name string, price double, ts bigint)
                  using hudi location 'hdfs://10.0.0.180:9000/scala/%s'
                  tblproperties (type='cow', primaryKey='id', preCombineField = 'ts')"""
//    spark.sql(createPlaceHolder.format(tableName, tableName));
//
//    var i = 0;
//    val insertPlaceHolder = "insert into %s select %d, 'zzt1', 1.11, 11111"
//    for(i <- 1 until 20) {
//      val insert = insertPlaceHolder.format(tableName, i);
//      spark.sql(insert);
//    };

    val updatePlaceHolder = "update %s set price = price * 2, ts=11116 where id >= %d and id <= %d";
    val update = updatePlaceHolder.format(tableName, 2, 17);
    spark.sql(update);

    val queryPlaceHolder = "select * from %s";
    spark.sql(queryPlaceHolder.format(tableName)).show();

    val showTable = "show tables";
    spark.sql(showTable).show();
  }
}
