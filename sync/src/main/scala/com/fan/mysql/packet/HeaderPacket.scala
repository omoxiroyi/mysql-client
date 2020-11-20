package com.fan.mysql.packet

import org.apache.commons.lang3.builder.{ToStringBuilder, ToStringStyle}

/** <pre>
  * Offset  Length     Description
  * 0       3        Packet body length stored with the low byte first.
  * 3       1        Packet sequence number. The sequence numbers are reset with each new command.
  * While the correct packet sequencing is ensured by the underlying transmission protocol,
  * this field is used for the sanity checks of the application logic.
  * </pre>
  *
  * <br>
  * The Packet Header will not be shown in the descriptions of packets that
  * follow this section. Think of it as always there. But logically, it
  * "precedes the packet" rather than "is included in the packet".<br>
  *
  * @author fan
  */
class HeaderPacket {

  /** this field indicates the packet length that follows the header, with
    * header packet's 4 bytes excluded.
    */
  var packetBodyLength: Int = _

  var packetSequenceNumber: Byte = _

  def toBytes: Array[Byte] = {
    val data = new Array[Byte](4)
    data(0) = (packetBodyLength & 0xff).asInstanceOf[Byte]
    data(1) = (packetBodyLength >>> 8).asInstanceOf[Byte]
    data(2) = (packetBodyLength >>> 16).asInstanceOf[Byte]
    data(3) = packetSequenceNumber
    data
  }

  def fromBytes(data: Array[Byte]): Unit = {
    if (data == null || data.length < 4)
      throw new IllegalArgumentException(
        "invalid header data. It can't be null and the length must be 4 byte."
      )
    this.packetBodyLength = (data(0) & 0xff) | ((data(1) & 0xff) << 8) | ((data(2) & 0xff) << 16)
    this.packetSequenceNumber = data(3)
  }

  override def toString: String =
    ToStringBuilder.reflectionToString(this, ToStringStyle.DEFAULT_STYLE)
}

object HeaderPacket {

  def fromBytes(data: Array[Byte]): HeaderPacket = {
    val headerPacket = new HeaderPacket
    headerPacket.fromBytes(data)
    headerPacket
  }
}
