package com.fan.mysql.async.binlog

import java.util

import com.fan.mysql.async.binlog.event.impl.{FormatDescriptionEvent, TableMapEvent}
import com.fan.mysql.async.util.MySQLConstants

class BinlogDumpContext() {
  private[this] var semiSync: Boolean = false
  private[this] var needReply: Boolean = false
  private[this] var timeZone: String = _
  private[this] var binlogFileName: String = _
  private[this] var binlogPosition: Long = 0L
  private[this] var formatDescription = new FormatDescriptionEvent(
    MySQLConstants.BINLOG_CHECKSUM_ALG_OFF)

  private[this] val tableMapEvents = new util.HashMap[Long, TableMapEvent]

  def this(binlogChecksum: Int) = {
    this()
    this.formatDescription = new FormatDescriptionEvent(binlogChecksum)
  }

  def isSemiSync: Boolean = semiSync

  def setSemiSync(semiSync: Boolean): Unit = {
    this.semiSync = semiSync
  }

  def isNeedReply: Boolean = needReply

  def setNeedReply(needReply: Boolean): Unit = {
    this.needReply = needReply
  }

  def getBinlogFileName: String = binlogFileName

  def setBinlogFileName(binlogFileName: String): Unit = {
    this.binlogFileName = binlogFileName
  }

  def getBinlogPosition: Long = binlogPosition

  def setBinlogPosition(binlogPosition: Long): Unit = {
    this.binlogPosition = binlogPosition
  }

  def getFormatDescription: FormatDescriptionEvent = formatDescription

  def setFormatDescription(formatDescription: FormatDescriptionEvent): Unit = {
    this.formatDescription = formatDescription
  }

  def getTableMapEvents: util.Map[Long, TableMapEvent] = tableMapEvents

  def getTimeZone: String = timeZone

  def setTimeZone(timeZone: String): Unit = {
    this.timeZone = timeZone
  }

}
