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

package com.oceanbase.spark.utils

import org.apache.spark.internal.Logging
import org.apache.spark.sql.execution.datasources.jdbc.JDBCOptions
import org.apache.spark.sql.jdbc.JdbcConnectionProvider

import java.sql.{Connection, Driver}
import java.util.Properties

import scala.collection.JavaConverters._

object OceanBaseConnectionProvider extends JdbcConnectionProvider with Logging {

  /** Additional properties for data connection (Data source property takes precedence). */
  private def getAdditionalProperties(options: JDBCOptions): Properties = new Properties()

  override val name: String = "OceanBase"

  override def canHandle(driver: Driver, options: Map[String, String]): Boolean = {
    val jdbcOptions = new JDBCOptions(options)
    jdbcOptions.keytab == null || jdbcOptions.principal == null
  }

  override def getConnection(driver: Driver, options: Map[String, String]): Connection = {
    val jdbcOptions = new JDBCOptions(options)
    val properties = getAdditionalProperties(jdbcOptions)
    jdbcOptions.asConnectionProperties.asScala.foreach {
      case (k, v) =>
        properties.put(k, v)
    }
    logDebug(s"JDBC connection initiated with URL: ${jdbcOptions.url} and properties: $properties")
    driver.connect(jdbcOptions.url, properties)
  }

  /** Note: Do not add the override keyword to ensure compatibility with older Spark versions. */
  def modifiesSecurityContext(
      driver: Driver,
      options: Map[String, String]
  ): Boolean = {
    // OceanBaseConnectionProvider is the default unsecure connection provider, so just return false
    false
  }
}
