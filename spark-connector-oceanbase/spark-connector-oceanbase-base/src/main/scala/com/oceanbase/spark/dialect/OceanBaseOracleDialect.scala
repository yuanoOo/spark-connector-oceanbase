/*
 * Copyright 2024 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.oceanbase.spark.dialect

import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.datasources.jdbc.{JDBCOptions, JdbcOptionsInWrite}
import org.apache.spark.sql.types.StructType

import java.sql.{Connection, Date, Timestamp}
import java.util

import scala.collection.mutable.ArrayBuffer

class OceanBaseOracleDialect extends OceanBaseDialect {
  override def quoteIdentifier(colName: String): String = {
    s""""$colName""""
  }

  override def createTable(
      conn: Connection,
      tableName: String,
      schema: StructType,
      partitions: Array[Transform],
      options: JdbcOptionsInWrite,
      properties: util.Map[String, String]): Unit =
    throw new UnsupportedOperationException("Not currently supported in oracle mode")

  /** Creates a schema. */
  override def createSchema(
      conn: Connection,
      options: JDBCOptions,
      schema: String,
      comment: String): Unit =
    throw new UnsupportedOperationException("Not currently supported in oracle mode")

  override def schemaExists(conn: Connection, options: JDBCOptions, schema: String): Boolean =
    throw new UnsupportedOperationException("Not currently supported in oracle mode")

  override def listSchemas(conn: Connection, options: JDBCOptions): Array[Array[String]] =
    throw new UnsupportedOperationException("Not currently supported in oracle mode")

  /** Drops a schema from OceanBase. */
  override def dropSchema(
      conn: Connection,
      options: JDBCOptions,
      schema: String,
      cascade: Boolean): Unit = throw new UnsupportedOperationException(
    "Not currently supported in oracle mode")

  override def getPriKeyInfo(
      schemaName: String,
      tableName: String,
      option: JDBCOptions): ArrayBuffer[PriKeyColumnInfo] = {
    throw new UnsupportedOperationException("Not currently supported in oracle mode")
  }

  override def getInsertIntoStatement(tableName: String, schema: StructType): String = {
    throw new UnsupportedOperationException("Not currently supported in oracle mode")
  }

  override def getUpsertIntoStatement(
      tableName: String,
      schema: StructType,
      priKeyColumnInfo: ArrayBuffer[PriKeyColumnInfo]): String = {
    throw new UnsupportedOperationException("Not currently supported in oracle mode")
  }

  override def compileValue(value: Any): Any = value match {
    // The JDBC drivers support date literals in SQL statements written in the
    // format: {d 'yyyy-mm-dd'} and timestamp literals in SQL statements written
    // in the format: {ts 'yyyy-mm-dd hh:mm:ss.f...'}. For details, see
    // 'Oracle Database JDBC Developer’s Guide and Reference, 11g Release 1 (11.1)'
    // Appendix A Reference Information.
    case stringValue: String => s"'${escapeSql(stringValue)}'"
    case timestampValue: Timestamp => "{ts '" + timestampValue + "'}"
    case dateValue: Date => "{d '" + dateValue + "'}"
    case arrayValue: Array[Any] => arrayValue.map(compileValue).mkString(", ")
    case _ => value
  }
}
