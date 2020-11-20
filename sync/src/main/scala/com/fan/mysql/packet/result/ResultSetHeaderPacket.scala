package com.fan.mysql.packet.result

import com.fan.mysql.packet.MySQLPacket
import com.fan.mysql.util.MySQLPacketBuffer

/** * From server to client after command, if no error and result set -- that is,
  * if the command was a query which returned a result set. The Result Set Header
  * Packet is the first of several, possibly many, packets that the server sends
  * for result sets. The order of packets for a result set is:
  *
  * <pre>
  * (Result Set Header Packet)   the number of columns
  * (Field Packets)              column descriptors
  * (EOF Packet)                 marker: end of Field Packets
  * (Row Data Packets)           row contents
  * (EOF Packet)                 marker: end of Data Packets
  *
  * Bytes                        Name
  * -----                        ----
  * 1-9   (Length-Coded-Binary)  field_count
  * 1-9   (Length-Coded-Binary)  extra
  *
  * &#64;see http://forge.mysql.com/wiki/MySQL_Internals_ClientServer_Protocol#Result_Set_Header_Packet
  * </pre>
  *
  * @author fan
  */
class ResultSetHeaderPacket extends MySQLPacket {

  var fieldCount: Int = _
  var extra: Long     = _

  override def init(buffer: MySQLPacketBuffer, charset: String): Unit = {
    super.init(buffer, charset)
    this.fieldCount = buffer.readLength.toInt
    if (buffer.hasRemaining) this.extra = buffer.readLength
  }

  override def write2Buffer(buffer: MySQLPacketBuffer): Unit = {
    buffer.writeLength(this.fieldCount)
    if (this.extra > 0) buffer.writeLength(this.extra)
  }

  override def calcPacketSize: Int = {
    var size = super.calcPacketSize
    size += MySQLPacketBuffer.getLength(this.fieldCount)
    if (this.extra > 0) size += MySQLPacketBuffer.getLength(this.extra)
    size
  }

  override def getPacketInfo = "MySQL ResultSetHeader Packet"
}
