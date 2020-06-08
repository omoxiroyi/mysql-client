package com.fan.mysql.packet.command

import com.fan.mysql.packet.CommandPacket

object QuitPacket extends CommandPacket {
  commandType = CommandPacket.COM_QUERY

  override def getPacketInfo = "MySQL Quit Packet"
}
