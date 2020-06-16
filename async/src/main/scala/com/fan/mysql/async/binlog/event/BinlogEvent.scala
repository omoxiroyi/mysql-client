package com.fan.mysql.async.binlog.event

import com.fan.mysql.async.message.server.ServerMessage

trait BinlogEvent extends ServerMessage {

  override val kind: Int = ServerMessage.BinlogEvent

  def getEventHeader: EventHeader

  def setOriginData(originData: Array[Byte]): Unit

  def getOriginData: Array[Byte]
}
