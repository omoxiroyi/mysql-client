package com.fan.mysql.packet

import java.io.UnsupportedEncodingException

import com.fan.mysql.util.MySQLPacketBuffer

/** From server to client in response to command, if no error and no result set.
  *
  * <pre>
  * VERSION 4.1
  * Bytes                       Name
  * -----                       ----
  * 1   (Length Coded Binary)   field_count, always = 0
  * 1-9 (Length Coded Binary)   affected_rows
  * 1-9 (Length Coded Binary)   insert_id
  * 2                           server_status
  * 2                           warning_count
  * n   (until end of packet)   message
  *
  * field_count:     always = 0
  *
  * affected_rows:   = number of rows affected by INSERT/UPDATE/DELETE
  *
  * insert_id:       If the statement generated any AUTO_INCREMENT number,
  * it is returned here. Otherwise this field contains 0.
  * Note: when using for example a multiple row INSERT the
  * insert_id will be from the first row inserted, not from
  * last.
  *
  * server_status:   = The client can use this to check if the
  * command was inside a transaction.
  *
  * warning_count:   number of warnings
  *
  * message:         For example, after a multi-line INSERT, message might be
  * &quot;Records: 3 Duplicates: 0 Warnings: 0&quot;
  * </pre>
  *
  * @see https://dev.mysql.com/doc/internals/en/packet-OK_Packet.html
  * @author fan
  */
class OkPacket extends MySQLPacket {

  var fieldCount: Byte   = OkPacket.FIELD_COUNT
  var affectedRows: Long = _
  var insertId: Long     = _
  var serverStatus: Int  = _
  var warningCount: Int  = _
  var message: String    = _

  override def init(buffer: MySQLPacketBuffer, charset: String): Unit = {
    super.init(buffer, charset)
    this.fieldCount = buffer.read
    this.affectedRows = buffer.readLength
    this.insertId = buffer.readLength
    this.serverStatus = buffer.readUB2
    this.warningCount = buffer.readUB2
    if (buffer.hasRemaining) this.message = buffer.readStringWithLength
  }

  override def write2Buffer(buffer: MySQLPacketBuffer): Unit = {
    buffer.write(this.fieldCount)
    buffer.writeLength(this.affectedRows)
    buffer.writeLength(this.insertId)
    buffer.writeUB2(this.serverStatus)
    buffer.writeUB2(this.warningCount)
    if (this.message != null) buffer.writeStringWithLength(this.message)
  }

  override def calcPacketSize: Int = {
    var size = super.calcPacketSize
    size += 1
    size += MySQLPacketBuffer.getLength(this.affectedRows)
    size += MySQLPacketBuffer.getLength(this.insertId)
    size += 4
    if (this.message != null)
      try {
        val msg =
          if (this.charset == null) message.getBytes
          else message.getBytes(this.charset)
        size += MySQLPacketBuffer.getLength(msg)
      } catch {
        case _: UnsupportedEncodingException =>
      }
    size
  }

  override def getPacketInfo = "MySQL OK Packet"
}

object OkPacket {
  final val FIELD_COUNT: Byte = 0x00

  final val OK: OkPacket = new OkPacket
  OK.packetId = 1
  OK.affectedRows = 0
  OK.insertId = 0
  OK.serverStatus = 2
  OK.warningCount = 0
}
