package com.fan.mysql.event

trait EventHeader {
  def getHeaderLength: Int

  def getPosition: Long

  def getTimestamp: Long

  def getEventType: Int

  def getServerId: Long

  def getEventLength: Long

  def getNextPosition: Long

  def getFlags: Int

  def getBinlogFileName: String

  def getTimestampOfReceipt: Long

  def getChecksumAlg: Int

  def setBinlogFileName(binlogFileName: String): Unit

  def setChecksumAlg(checksumAlg: Int): Unit

  def setEventLength(eventLength: Long): Unit

  def setEventType(eventType: Int): Unit

  def setFlags(flags: Int): Unit

  def setNextPosition(nextPosition: Long): Unit

  def setServerId(serverId: Long): Unit

  def setTimestamp(timestamp: Long): Unit

  def setTimestampOfReceipt(timestampOfReceipt: Long): Unit

  def getByteArray: Array[Byte]
}
