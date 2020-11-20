package com.fan.mysql.packet

import java.io.UnsupportedEncodingException

import com.fan.mysql.util.MySQLPacketBuffer

/** From server to client in response to command, if error.
  *
  * <pre>
  * Bytes                       Name
  * -----                       ----
  * 1                           field_count, always = 0xff
  * 2                           errno
  * 1                           (sqlstate marker), always '#'
  * 5                           sqlstate (5 characters)
  * n                           message
  *
  * &#64;see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Error_Packet
  * </pre>
  *
  * @author fan
  */
class ErrorPacket extends MySQLPacket {

  val FIELD_COUNT: Byte = 0xff.toByte

  private[this] val SQLSTATE_MARKER: Byte    = '#'.toByte
  private[this] val DEFAULT_SQLSTATE: String = "42000"

  var fieldCount: Byte = FIELD_COUNT
  var errNo            = 0
  var mark: Byte       = SQLSTATE_MARKER
  var sqlState: String = DEFAULT_SQLSTATE
  var message: String  = _

  override def init(buffer: MySQLPacketBuffer, charset: String): Unit = {
    super.init(buffer, charset)
    this.fieldCount = buffer.read
    this.errNo = buffer.readUB2
    if (buffer.hasRemaining && (buffer.read(buffer.getPosition) == SQLSTATE_MARKER)) {
      buffer.read
      this.sqlState = buffer.readLengthString(5)
    }
    this.message = buffer.readSting
  }

  override def write2Buffer(buffer: MySQLPacketBuffer): Unit = {
    buffer.write(this.fieldCount)
    buffer.writeUB2(this.errNo)
    buffer.write(this.mark)
    buffer.writeStringNoNull(this.sqlState)
    if (this.message != null) buffer.writeStringNoNull(this.message)
  }

  override def calcPacketSize: Int = {
    var size: Int = super.calcPacketSize
    size += 9 // 1+2+1+5

    if (message != null)
      try {
        size += (if (this.charset == null) message.getBytes.length
                 else message.getBytes(this.charset).length)
      } catch {
        case _: UnsupportedEncodingException =>
      }
    size
  }

  override def getPacketInfo = "MySQL Error Packet"
}
