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

case class Nation (N_NATIONKEY: Integer, N_NAME: String, N_REGIONKEY: Integer, N_COMMENT: String)

case class Part(P_PARTKEY: Integer, P_NAME: String, P_MFGR: String, P_BRAND: String, P_TYPE: String, P_SIZE: Integer, P_CONTAINER: String, P_RETAILPRICE: Double, P_COMMENT: String)

case class Supplier(S_SUPPKEY: Integer, S_NAME: String, S_ADDRESS: String, S_NATIONKEY: Integer, S_PHONE: String, S_ACCTBAL: Double, S_COMMENT: String)

case class Partsupp(PS_PARTKEY: Integer, PS_SUPPKEY: Integer, PS_AVAILQTY: Integer, PS_SUPPLYCOST: Double, PS_COMMENT: String)

case class Customer(C_CUSTKEY: Integer, C_NAME: String, C_ADDRESS: String, C_NATIONKEY: Integer, C_PHONE: String, C_ACCTBAL: Double, C_MKTSEGMENT: String, C_COMMENT: String)

case class Orders(O_ORDERKEY: Integer, O_CUSTKEY: Integer, O_ORDERSTATUS: String, O_TOTALPRICE: Double, O_ORDERDATE: String, O_ORDERPRIORITY: String, O_CLERK: String, O_SHIPPRIORITY: Integer, O_COMMENT: String)

case class LineItem(L_ORDERKEY: Integer, L_PARTKEY: Integer, L_SUPPKEY: Integer, L_LINENUMBER: Integer, L_QUANTITY: Double, L_EXTENDEDPRICE: Double, L_DISCOUNT: Double, L_TAX: Double, L_RETURNFLAG: String, L_LINESTATUS: String, L_SHIPDATE: String, L_COMMITDATE: String, L_RECEIPTDATE: String, L_SHIPINSTRUCT: String, L_SHIPMODE: String, L_COMMENT: String)

case class Region(R_REGIONKEY: Integer, R_NAME: String, R_COMMENT: String)


