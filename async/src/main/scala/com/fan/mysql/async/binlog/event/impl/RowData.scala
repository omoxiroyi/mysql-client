package com.fan.mysql.async.binlog.event.impl

import java.util

import com.fan.mysql.async.binlog.event.EventColumn
import org.apache.commons.lang3.builder.{ToStringBuilder, ToStringStyle}

class RowData {
  private var beforeColumns: Array[EventColumn] = _
  private var afterColumns: Array[EventColumn]  = _
  private var beforeBit: util.BitSet            = _
  private var afterBit: util.BitSet             = _

  def getBeforeColumns: Array[EventColumn] = beforeColumns

  def setBeforeColumns(beforeColumns: Array[EventColumn]): Unit = {
    this.beforeColumns = beforeColumns
  }

  def getAfterColumns: Array[EventColumn] = afterColumns

  def setAfterColumns(afterColumns: Array[EventColumn]): Unit = {
    this.afterColumns = afterColumns
  }

  def getBeforeBit: util.BitSet = beforeBit

  def setBeforeBit(beforeBit: util.BitSet): Unit = {
    this.beforeBit = beforeBit
  }

  def getAfterBit: util.BitSet = afterBit

  def setAfterBit(afterBit: util.BitSet): Unit = {
    this.afterBit = afterBit
  }

  override def toString: String =
    ToStringBuilder.reflectionToString(this, ToStringStyle.DEFAULT_STYLE)
}
