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

import scala.collection.mutable.Map
import org.apache.spark.internal.Logging
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.AliasIdentifier
import org.apache.spark.sql.catalyst.expressions.NamedExpression
import org.apache.spark.sql.catalyst.plans.logical.{LogicalPlan, Project, SubqueryAlias}
import org.apache.spark.sql.execution.{QueryExecution, SparkPlan}
import org.apache.spark.sql.mv.{SchemaRegistry, ViewCatalyst}


object SparkHudiSQL extends Logging{
  def main(args: Array[String]): Unit = {
    val mysqlUrl = "rm-uf67ktcrjo69g32viko.mysql.rds.aliyuncs.com:3306/metastore";
    val mysqlPlaceHolder = "jdbc:mysql://%s?createDatabaseIfNotExist=true";
    val spark = SparkSession.builder()
      .appName("Spark Debug")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .config("spark.sql.extensions", "org.apache.spark.sql.hudi.HoodieSparkSessionExtension")
      .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/warehouse")
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
    initDb(spark);
    // runCreateTable(spark);
  }

  private def initDb(spark: SparkSession): Unit = {
    ViewCatalyst.createViewCatalyst()
    var schemaRegistry: SchemaRegistry = new SchemaRegistry(spark);
  }

  private def runCreateTable(spark: SparkSession): Unit = {
//    spark.sql("""CREATE TABLE IF NOT EXISTS src (key INT, value STRING)
//              USING hive location 'hdfs://10.0.0.180:9000/warehouse/%s'""")

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
//    spark.sql(update);

    val queryPlaceHolder = "select * from %s";
    val df: DataFrame = spark.sql(queryPlaceHolder.format(tableName));

    val queryExecution: QueryExecution = df.queryExecution;
    val analyzed = queryExecution.analyzed;
    val optimizedPlan: LogicalPlan = queryExecution.optimizedPlan;
    val sparkPlan: SparkPlan = queryExecution.sparkPlan;
    val executedPlan: SparkPlan = queryExecution.executedPlan;

    val project: Project = analyzed.asInstanceOf[Project];
    val child: SubqueryAlias = project.child.asInstanceOf[SubqueryAlias];
    val identifier: AliasIdentifier = child.identifier;
    val database = identifier.qualifier(1);
    val name = identifier.name;
    val projectList: Seq[NamedExpression] = project.projectList;

    val attributions: Map[String, String] = Map();
    projectList.foreach { e => {
        if (!e.name.contains("_hoodie_")) {
          attributions += (e.name -> e.dataType.simpleString)
        }
      }
    }

    spark.sql("use metadata_");
    val hivePlaceHolder = """create table if not exists %s
                            ( attribution string, usage_count int)
                            using hive location 'hdfs://10.0.0.180:9000/warehouse/%s' """;
    val hiveTableName = "metadata_.%s__%s".format(database, name);
    spark.sql(hivePlaceHolder.format(hiveTableName, hiveTableName));
    attributions.foreach { p => {
        val initPlaceHolder = "insert into %s select '%s', 0";
        spark.sql(initPlaceHolder.format(hiveTableName, p._1));
      }
    }


    val data: Array[Row] = df.collect();
    df.show();

    val showTable = "show tables";
    spark.sql(showTable).show();
  }
}
