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
package com.oceanbase.spark.writer.v2

import com.oceanbase.spark.config.OceanBaseConfig
import com.oceanbase.spark.dialect.OceanBaseDialect
import com.oceanbase.spark.utils.{OBJdbcUtils, RetryUtils}
import com.oceanbase.spark.utils.OBJdbcUtils.OBValueSetter

import org.apache.spark.internal.Logging
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.metric.CustomTaskMetric
import org.apache.spark.sql.connector.write.{DataWriter, WriterCommitMessage}
import org.apache.spark.sql.types.StructType

import java.sql.{Connection, SQLException, Types}

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

class JDBCWriter(schema: StructType, config: OceanBaseConfig, dialect: OceanBaseDialect)
  extends DataWriter[InternalRow]
  with Logging {

  private val DEFAULT_BUFFER_SIZE = config.getJdbcBatchSize
  val buffer: ArrayBuffer[InternalRow] = ArrayBuffer[InternalRow]()
  lazy val conn: Connection = OBJdbcUtils.getConnection(config)
  lazy val sql: String = getInsertSql
  private val setters: Array[OBValueSetter] =
    schema.fields.map(f => OBJdbcUtils.makeSetter(f.dataType))

  override def write(record: InternalRow): Unit = {
    // Added copy() method to fix data duplication issue caused by CTAS syntax
    buffer += record.copy()
    if (buffer.length >= DEFAULT_BUFFER_SIZE) flush()
  }

  private def flush(): Unit = {
    RetryUtils.retry() {
      Try {
        doFlush()
      } match {
        case Success(_) => buffer.clear()
        case Failure(exception) => throw exception
      }
    }
  }

  private def doFlush(): Unit = {
    if (buffer.isEmpty) return

    var committed = false
    try {
      if (config.getJdbcEnableAutoCommit) {
        conn.setAutoCommit(true)
      } else {
        conn.setAutoCommit(false)
      }
      conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED)
      val statement = conn.prepareStatement(sql)
      try {
        buffer.foreach {
          row =>
            {
              schema.fieldNames.zipWithIndex.foreach {
                case (_, index) =>
                  if (row.isNullAt(index)) {
                    statement.setNull(index + 1, Types.NULL)
                  } else {
                    setters(index).apply(statement, row, index)
                  }
              }
              statement.addBatch()
            }
        }
        statement.executeBatch()
      } finally {
        statement.close()
      }
      if (!config.getJdbcEnableAutoCommit) {
        conn.commit()
        committed = true
      }
    } catch {
      case ex: SQLException =>
        throw new RuntimeException(s"Failed to execute batch with sql: $sql", ex)
    } finally {
      if (!config.getJdbcEnableAutoCommit && !committed) {
        conn.rollback()
      }
    }
  }

  private def getInsertSql: String = {
    val priKeyColInfos =
      dialect.getPriKeyInfo(conn, config.getSchemaName, config.getTableName, config) match {
        case cols if cols.nonEmpty => cols
        case _ =>
          dialect.getUniqueKeyInfo(conn, config.getSchemaName, config.getTableName, config)
      }

    val tableName = config.getDbTable
    if (null != priKeyColInfos && priKeyColInfos.nonEmpty) {
      dialect.getUpsertIntoStatement(tableName, schema, priKeyColInfos)
    } else {
      dialect.getInsertIntoStatement(tableName, schema)
    }
  }

  override def commit(): WriterCommitMessage = {
    // Do flush
    flush()
    CommitMessage()
  }

  override def abort(): Unit = {}

  override def close(): Unit = {
    if (null != conn) {
      conn.close()
      logInfo("The connection of task has been closed.")
    }
  }

  override def currentMetricsValues(): Array[CustomTaskMetric] = {
    super.currentMetricsValues()
  }
}
