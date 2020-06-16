package com.fan.mysql.async.binlog.event.impl

import com.fan.mysql.async.binlog.event.{AbstractEvent, EventHeader}

class RotateEvent(header: EventHeader) extends AbstractEvent(header) {
  private[this] var binlogPosition: Long = _
  private[this] var binlogFile: String = _

  def getBinlogPosition: Long = binlogPosition

  def getBinlogFile: String = binlogFile

  def setBinlogPosition(binlogPosition: Long): Unit = {
    this.binlogPosition = binlogPosition
  }

  def setBinlogFile(binlogFile: String): Unit = {
    this.binlogFile = binlogFile
  }
}
