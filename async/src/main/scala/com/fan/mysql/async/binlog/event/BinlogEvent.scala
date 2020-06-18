package com.fan.mysql.async.binlog.event

import com.fan.mysql.async.message.server.ServerMessage
import org.apache.commons.lang3.builder.{ToStringBuilder, ToStringStyle}

abstract class BinlogEvent extends ServerMessage(ServerMessage.BinlogEvent) {

  def getEventHeader: EventHeader

  def setOriginData(originData: Array[Byte]): Unit

  def getOriginData: Array[Byte]

  override def toString: String =
    ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE)
}
