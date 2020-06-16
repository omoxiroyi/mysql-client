package com.fan.mysql.async.binlog.event

abstract class AbstractEvent(header: EventHeader) extends BinlogEvent {

  protected var originData: Array[Byte] = _

  override def getEventHeader: EventHeader = header

  override def getOriginData: Array[Byte] = originData

  override def setOriginData(originData: Array[Byte]): Unit = {
    this.originData = originData
  }

}
