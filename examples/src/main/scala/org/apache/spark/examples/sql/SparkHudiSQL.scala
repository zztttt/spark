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
import org.apache.spark.sql.mv.{MVOptimizRewrite, SchemaRegistry, ViewCatalyst}


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
    // test(spark);
    // initDb(spark);
    runHudi(spark);
    // runCreateTable(spark);
  }

  private def test(spark: SparkSession): Unit = {
    // spark.sql("drop table depts_hudi");

    spark.sql("show tables").show();
    spark.sql("select * from depts_hudi").show();
    spark.sql("select * from emps_hudi").show();
  }

  private def initDb(spark: SparkSession): Unit = {
    ViewCatalyst.createViewCatalyst()
    var schemaRegistry: SchemaRegistry = new SchemaRegistry(spark);

    schemaRegistry.createTableInHudi(
      "depts_hudi",
      """create table if not exists depts_hudi(
        | deptno INT NOT NULL,
        | deptname STRING
        | ) using hudi
        | location 'hdfs://10.0.0.203:9000/hudi/depts_hudi'
        | tblproperties(
        |   type='mor',
        |   primaryKey='deptno'
        | )
        |""".stripMargin
    )
    schemaRegistry.createTableInHudi(
      "emps_hudi",
      """ create table if not exists emps_hudi(
        | empid INT NOT NULL,
        | deptno INT NOT NULL,
        | locationsid INT NOT NULL,
        | empname STRING NOT NULL,
        | salary DECIMAL (18,2)
        |) using hudi
        | location 'hdfs://10.0.0.203:9000/hudi/emps_hudi'
        | tblproperties(
        |   type='mor',
        |   primaryKey='empid'
        | )
        |""".stripMargin
    )

    val insert1 = "insert into table depts_hudi values(1, '10'),(2, '20'),(3, '30'),(4, '40')";
    spark.sql(insert1);
    val insert2 = "insert into table emps_hudi values(10,1,0,'name',0),(20,2,0,'name',0),(30,3,0,'name',0),(40,4,0,'name',0)";
    spark.sql(insert2);
  }

  private def runHudi(spark: SparkSession): Unit = {
    ViewCatalyst.createViewCatalyst()
    val schemaRegistry: SchemaRegistry = new SchemaRegistry(spark);
    schemaRegistry.createMV("emps_mv",
      """SELECT empid
        |FROM emps_hudi JOIN depts_hudi ON depts_hudi.deptno = emps_hudi.deptno
        |""".stripMargin
    )

    var startTime = System.currentTimeMillis();
    spark.sql(
      """SELECT empid
        |FROM emps_hudi JOIN depts_hudi ON depts_hudi.deptno = emps_hudi.deptno
        |WHERE emps_hudi.empid > 10
        |""".stripMargin).show()
    var endTime = System.currentTimeMillis()
    val time1 = endTime - startTime

    startTime = System.currentTimeMillis()
    val rewrite = MVOptimizRewrite.execute(schemaRegistry.toLogicalPlan(
      """SELECT empid
        |FROM emps_hudi JOIN depts_hudi ON depts_hudi.deptno = emps_hudi.deptno
        |WHERE emps_hudi.empid > 10
        |""".stripMargin))
    endTime = System.currentTimeMillis();
    val time2 = endTime - startTime

    startTime = System.currentTimeMillis();
    spark.logicalPlanSql(rewrite).show();
    endTime = System.currentTimeMillis();
    val time3 = endTime - startTime

    logInfo(String.valueOf(time1))
    logInfo(String.valueOf(time2))
    logInfo(String.valueOf(time3))

    // scala.io.StdIn.readLine()
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
