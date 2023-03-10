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
package org.apache.spark.sql.mv

import com.google.gson.{Gson, JsonArray, JsonObject}
import org.apache.http.HttpResponse
import org.apache.http.client.methods.{HttpGet, HttpPost}
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.DefaultHttpClient
import org.apache.http.util.EntityUtils

import scala.collection.mutable.ListBuffer
import scala.io.Source

object MetadataClient {
  val url = "http://localhost:8080"
  val httpClient = new DefaultHttpClient()
  val gson = new Gson()

  def createMV(viewName: String, createSql: String, types: String): Unit = {
    val data: JsonObject = new JsonObject();
    data.addProperty("viewName", viewName)
    data.addProperty("createSql", createSql)
    data.addProperty("columnTypes", types)
    data.addProperty("expire", 0)
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
    gson.fromJson(result, classOf[JsonObject])
  }

  def loadCandidate(sql: String): Seq[String] = {
    val data: JsonObject = new JsonObject();
    data.addProperty("sql", sql)

    val post = new HttpPost(url + "/spark/loadCandidate")
    post.addHeader("Content-Type", "application/json;charset=UTF-8")
    post.setEntity(new StringEntity(data.toString()))
    val httpEntity = httpClient.execute(post).getEntity
    val result = Source.fromInputStream(httpEntity.getContent).getLines().mkString("\n")

    val ret = ListBuffer[String]()
    val a = gson.fromJson(result, classOf[JsonArray])
    gson.fromJson(result, classOf[JsonArray]).forEach(f => {
      ret.append(f.getAsString)
    })
    ret
  }

  def loadAllMV(): JsonObject = {
    val get = new HttpGet(url + "/loadAllMV")
    sendGetRequest(get)
  }

  def sendGetRequest(get: HttpGet): JsonObject = {
    val result = EntityUtils.toString(httpClient.execute(get).getEntity)
    gson.fromJson(result, classOf[JsonObject])
  }
}
