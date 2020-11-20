package com.fan.mysql.async.binlog.event.impl

import java.util

import com.fan.mysql.async.binlog.event.{AbstractEvent, EventHeader}

class RowDataEvent(header: EventHeader, tableId: Long) extends AbstractEvent(header) {

  private val rows: util.List[RowData] = new util.ArrayList[RowData](2)
  private var flags = 0

  def getFlags: Int = flags

  def setFlags(flags: Int): Unit = {
    this.flags = flags
  }

  def getTableId: Long = tableId

  def getRows: util.List[RowData] = rows

}
