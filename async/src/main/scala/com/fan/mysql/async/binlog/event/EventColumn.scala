package com.fan.mysql.async.binlog.event

import java.io.Serializable

import org.apache.commons.lang3.builder.{ToStringBuilder, ToStringStyle}

class EventColumn {
  private var columnValue: Serializable = _

  private var isNull = false

  def getColumnValue: Serializable = columnValue

  def setColumnValue(columnValue: Serializable): Unit = {
    this.columnValue = columnValue
  }

  def setNull(isNull: Boolean): Unit = {
    this.isNull = isNull
  }

  override def toString: String =
    ToStringBuilder.reflectionToString(this, ToStringStyle.DEFAULT_STYLE)

}
