package com.fan.mysql.packet.command

import java.io.UnsupportedEncodingException
import java.nio.ByteBuffer

import com.fan.mysql.packet.CommandPacket
import com.fan.mysql.util.MySQLPacketBuffer

/**
 * @author fan
 */
class QueryCommandPacket extends CommandPacket {
  var query: String = _

  override def init(buffer: MySQLPacketBuffer, charset: String): Unit = {
    super.init(buffer, charset)
    this.query = buffer.readSting
  }

  override def write2Buffer(buffer: MySQLPacketBuffer): Unit = {
    super.write2Buffer(buffer)
    buffer.writeStringNoNull(this.query)
  }

  override def calcPacketSize: Int = {
    var packSize = super.calcPacketSize
    if (query != null) try packSize += (if (this.charset == null) query.getBytes.length
    else query.getBytes(this.charset).length)
    catch {
      case e: UnsupportedEncodingException =>
        throw new RuntimeException(e)
    }
    packSize
  }

  override def toByteBuffer(charset: String): ByteBuffer = {
    setCharset(charset)
    val size = calcPacketSize
    val buffer = new MySQLPacketBuffer(size)
    buffer.init(charset)
    write2Buffer(buffer)
    afterPacketWritten(buffer)
    buffer.multiToByteBuffer
  }

  override def getPacketInfo = "MYSQL Query Packet"
}
