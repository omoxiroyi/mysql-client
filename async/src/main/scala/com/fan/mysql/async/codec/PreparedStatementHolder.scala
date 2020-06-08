

package com.fan.mysql.async.codec

import com.fan.mysql.async.message.server.{ColumnDefinitionMessage, PreparedStatementPrepareResponse}

import scala.collection.mutable.ArrayBuffer

class PreparedStatementHolder(val statement: String, val message: PreparedStatementPrepareResponse) {

  val columns = new ArrayBuffer[ColumnDefinitionMessage]
  val parameters = new ArrayBuffer[ColumnDefinitionMessage]

  def statementId: Array[Byte] = message.statementId

  def needsParameters: Boolean = message.paramsCount != this.parameters.length

  def needsColumns: Boolean = message.columnsCount != this.columns.length

  def needsAny: Boolean = this.needsParameters || this.needsColumns

  def add(column: ColumnDefinitionMessage) {
    if (this.needsParameters) {
      this.parameters += column
    } else {
      if (this.needsColumns) {
        this.columns += column
      }
    }
  }

  override def toString: String = s"PreparedStatementHolder($statement)"

}
