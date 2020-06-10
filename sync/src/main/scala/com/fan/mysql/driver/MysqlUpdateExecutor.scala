package com.fan.mysql.driver

import java.io.IOException
import java.nio.channels.SocketChannel

import com.fan.mysql.packet.command.QueryCommandPacket
import com.fan.mysql.packet.{CommandPacket, ErrorPacket, OkPacket}
import com.fan.mysql.util.{Logging, MySQLPacketBuffer, PacketManager}

class MysqlUpdateExecutor extends Logging {

  private var channel: SocketChannel = _
  private var charset = "utf-8"

  def this(connector: MysqlConnector) = {
    this()
    if (!connector.isConnected)
      throw new RuntimeException("should execute connector.connect() first")
    this.channel = connector.getChannel
    this.charset = connector.getCharset
  }

  def this(ch: SocketChannel) = {
    this()
    this.channel = ch
  }

  @throws[IOException]
  def update(updateString: String): OkPacket = {
    val cmd = new QueryCommandPacket
    cmd.packetId = 0
    cmd.commandType = CommandPacket.COM_QUERY
    cmd.query = updateString
    PacketManager.write(channel, cmd.toByteBuffer(charset).array)
    logger.debug("read update result...")
    val data = PacketManager.readNextPacket(channel)
    if (data(4) < 0) {
      val error = new ErrorPacket
      error.init(new MySQLPacketBuffer(data), charset)
      throw new IOException(s"${error.message}\n with command: $updateString")
    }
    val packet = new OkPacket
    packet.init(new MySQLPacketBuffer(data), charset)
    packet
  }
}
