package com.fan.mysql.async.binlog.event.impl

import java.util

import com.fan.mysql.async.binlog.event.{AbstractEvent, EventHeader}
import org.apache.commons.lang3.builder.{ToStringBuilder, ToStringStyle}

object TableMapEvent {

  final class ColumnInfo {
    var `type` = 0
    var meta = 0

    override def toString: String =
      ToStringBuilder.reflectionToString(this, ToStringStyle.DEFAULT_STYLE)
  }

}

class TableMapEvent(header: EventHeader) extends AbstractEvent(header) {

  import TableMapEvent._

  private[this] var dbname: String = _
  private[this] var tblname: String = _
  private[this] var columnCnt: Int = 0
  private[this] var columnInfo: Array[ColumnInfo] = _
  private[this] var tableId: Long = 0L
  private[this] var nullBits: util.BitSet = _

  def getDbname: String = dbname

  def setDbname(dbname: String): Unit = {
    this.dbname = dbname
  }

  def getTblname: String = tblname

  def setTblname(tblname: String): Unit = {
    this.tblname = tblname
  }

  def getColumnCnt: Int = columnCnt

  def setColumnCnt(columnCnt: Int): Unit = {
    this.columnCnt = columnCnt
  }

  def getColumnInfo: Array[TableMapEvent.ColumnInfo] = columnInfo

  def setColumnInfo(columnInfo: Array[TableMapEvent.ColumnInfo]): Unit = {
    this.columnInfo = columnInfo
  }

  def getTableId: Long = tableId

  def setTableId(tableId: Long): Unit = {
    this.tableId = tableId
  }

  def getNullBits: util.BitSet = nullBits

  def setNullBits(nullBits: util.BitSet): Unit = {
    this.nullBits = nullBits
  }
}
