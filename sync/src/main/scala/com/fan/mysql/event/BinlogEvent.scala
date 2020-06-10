package com.fan.mysql.event

trait BinlogEvent {
  def getEventHeader: EventHeader

  def setOriginData(originData: Array[Byte]): Unit

  def getOriginData: Array[Byte]
}
