package com.fan.mysql.driver

import java.io.IOException
import java.nio.channels.SocketChannel
import java.util

import com.fan.mysql.packet.command.QueryCommandPacket
import com.fan.mysql.packet.result.{FieldPacket, ResultSetHeaderPacket, ResultSetPacket, RowDataPacket}
import com.fan.mysql.packet.{CommandPacket, ErrorPacket}
import com.fan.mysql.util.{MySQLPacketBuffer, PacketManager}

class MysqlQueryExecutor {

  private[this] var channel: SocketChannel = _

  // use utf8 as default client charset
  private[this] var charset = "uft-8"

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
  def query(queryString: String): ResultSetPacket = {
    val cmd = new QueryCommandPacket
    cmd.packetId = 0
    cmd.commandType = CommandPacket.COM_QUERY
    cmd.query = queryString
    PacketManager.write(channel, cmd.toByteBuffer(charset).array)

    val data = PacketManager.readNextPacket(channel)

    if (data(4) == -1) {
      val packet = new ErrorPacket
      packet.init(new MySQLPacketBuffer(data), charset)
      throw new IOException(s"${packet.message}\n with command: $queryString")
    }

    val headerPacket = new ResultSetHeaderPacket
    headerPacket.init(new MySQLPacketBuffer(data), charset)

    val fields: util.List[FieldPacket] = new util.ArrayList[FieldPacket]

    for (_ <- 0 until headerPacket.fieldCount) {
      val fp: FieldPacket = new FieldPacket
      fp.init(new MySQLPacketBuffer(PacketManager.readNextPacket(channel)), charset)
      fields.add(fp)
    }

    readEofPacket()

    val rowData = new util.ArrayList[RowDataPacket]

    import scala.util.control.Breaks._

    // scala don't support native break statement.
    breakable {
      while (true) {
        val row = PacketManager.readNextPacket(channel)
        if (row(4) == -2)
          break
        val rp = new RowDataPacket(fields.size)
        rp.init(new MySQLPacketBuffer(row), charset)
        rowData.add(rp)
      }
    }

    val resultSet = new ResultSetPacket

    resultSet.getFieldDescriptors.addAll(fields)

    rowData.forEach { rp =>
      val row = new util.ArrayList[String]

      rp.fieldValues.forEach { bField =>
        if (bField == null) row.add(null)
        else {
          val col = new String(bField, charset)
          row.add(col)
        }
      }
      resultSet.addRow(row)
    }

    resultSet.setSourceAddress(channel.socket.getRemoteSocketAddress)
    resultSet
  }

  @throws[IOException]
  def query(queryString: String, queryHandler: MysqlConnQueryHandler): Unit = {
    val cmd = new QueryCommandPacket
    cmd.packetId = 0
    cmd.commandType = CommandPacket.COM_QUERY
    cmd.query = queryString
    PacketManager.write(channel, cmd.toByteBuffer(charset).array)
    val data = PacketManager.readNextPacket(channel)
    if (data(4) == -1) {
      val packet = new ErrorPacket
      packet.init(new MySQLPacketBuffer(data), charset)
      throw new IOException(packet.message + "\n with command: " + queryString)
    }
    val headerPacket = new ResultSetHeaderPacket
    headerPacket.init(new MySQLPacketBuffer(data), charset)
    val fields = new util.ArrayList[FieldPacket]
    for (_ <- 0 until headerPacket.fieldCount) {
      val fp = new FieldPacket
      fp.init(new MySQLPacketBuffer(PacketManager.readNextPacket(channel)), charset)
      fields.add(fp)
    }
    readEofPacket()
    queryHandler.onFieldPackets(fields)

    import scala.util.control.Breaks._

    // scala don't support native break statement.
    breakable {
      while (true) {
        val row = PacketManager.readNextPacket(channel)
        if (row(4) == -2) {
          queryHandler.onRowEof()
          break
        }
        val rp = new RowDataPacket(fields.size)
        rp.init(new MySQLPacketBuffer(row), charset)
        queryHandler.onRow(rp)
      }
    }

    val resultSet = new ResultSetPacket
    resultSet.getFieldDescriptors.addAll(fields)
  }

  @throws[IOException]
  private def readEofPacket(): Unit = {
    val eof = PacketManager.readNextPacket(channel)
    if (eof(4) != -2) throw new IOException("EOF Packet is expected, but packet with field_count=" + eof(4) + " is found.")
  }
}
