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
package org.apache.spark.examples.sql.httpclient

import com.google.gson.{Gson, JsonObject}
import org.apache.http.HttpResponse
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils

import scala.io.Source

object MetadataClient {
  val url = "http://localhost:8080"
  val httpClient = new DefaultHttpClient()
  def main(args: Array[String]): Unit = {
//    try {
//      val response = httpClient.execute(new HttpGet(url + "/getAll"))
//      val entity = response.getEntity
//      val result = Source.fromInputStream(entity.getContent).getLines().mkString("\n")
//      println(result)
//    } catch {
//      case ex: Exception => ex.printStackTrace()
//    }
//    val types: JSONArray = new JSONArray() {{
//      add(new JSONObject() {{
//        put("name", "col1")
//        put("type", "integer")
//      }} )
//      add(new JSONObject() {{
//        put("name", "col2")
//        put("type", "string")
//      }} )
//    }}

    // createMV("spark_test1", "spark test sql", types.toString())
    val ret = loadMV("spark_test1")
    println(ret.toString())
  }

  def createMV(viewName: String, createSql: String, types: String): Unit = {
    val data: JsonObject = new JsonObject();
    data.addProperty("viewName", viewName)
    data.addProperty("createSql", createSql)
    data.addProperty("columnTypes", types)
    val post = new HttpPost(url + "/create")
    post.addHeader("Content-Type", "application/json;charset=UTF-8")
    val entity: StringEntity = new StringEntity(data.toString())
    post.setEntity(entity)
    val response: HttpResponse = httpClient.execute(post)
    val httpEntity = response.getEntity
    val result = Source.fromInputStream(httpEntity.getContent).getLines().mkString("\n")
    println(result)
  }

  def loadMV(viewName: String): JsonObject = {
    val get = new HttpGet(url + "/loadMV?viewName=" + viewName)
    val response = httpClient.execute(get)
    val entity = response.getEntity
    val result = EntityUtils.toString(entity)
    val gson: Gson = new Gson()
    val ret: JsonObject = gson.fromJson(result, classOf[JsonObject])
    ret
  }
}
