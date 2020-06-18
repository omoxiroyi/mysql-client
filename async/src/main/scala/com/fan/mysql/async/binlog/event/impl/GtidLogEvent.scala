package com.fan.mysql.async.binlog.event.impl

import com.fan.mysql.async.binlog.event.{AbstractEvent, EventHeader}

class GtidLogEvent(header: EventHeader) extends AbstractEvent(header) {
  private[this] var sourceId: Array[Byte] = _
  private[this] var transactionId = 0L
  private[this] var logicalTimestamp = 0
  private[this] var lastCommitted = 0L
  private[this] var sequenceNumber = 0L

  def getSourceId: Array[Byte] = sourceId

  def setSourceId(sourceId: Array[Byte]): Unit = {
    this.sourceId = sourceId
  }

  def getTransactionId: Long = transactionId

  def setTransactionId(transactionId: Long): Unit = {
    this.transactionId = transactionId
  }

  def getLogicalTimestamp: Int = logicalTimestamp

  def setLogicalTimestamp(logicalTimestamp: Int): Unit = {
    this.logicalTimestamp = logicalTimestamp
  }

  def getLastCommitted: Long = lastCommitted

  def setLastCommitted(lastCommitted: Long): Unit = {
    this.lastCommitted = lastCommitted
  }

  def getSequenceNumber: Long = sequenceNumber

  def setSequenceNumber(sequenceNumber: Long): Unit = {
    this.sequenceNumber = sequenceNumber
  }

  def parseServerId: String = {
    val sb = new StringBuilder
    if (sourceId == null || sourceId.length <= 0) return null
    for (i <- sourceId.indices) {
      val v = sourceId(i) & 0xff
      val hv = Integer.toHexString(v)
      if (hv.length < 2) sb.append(0)
      sb.append(hv)
      if (i == 3 | i == 5 | i == 7 | i == 9) sb.append("-")
    }
    sb.toString
  }

}
