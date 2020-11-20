package com.fan.mysql.packet

import java.nio.ByteBuffer

import com.fan.mysql.util.{CharsetUtil, MySQLPacketBuffer}

abstract class MySQLPacket {

  /** Only present data length, not include header length */
  var packetLength: Int = _

  /** Current packet sequence number */
  var packetId: Byte = _

  /** Packet header length */
  protected final val HEADER_SIZE = 4

  protected var charset: String = _

  /** Instantiate the packet
    */
  def init(buffer: MySQLPacketBuffer, charset: String): Unit = {
    setCharset(charset)
    buffer.init(charset)
    buffer.setPosition(0)
    this.packetLength += buffer.read & 0xff
    this.packetLength += (buffer.read & 0xff) << 8
    this.packetLength += (buffer.read & 0xff) << 16
    this.packetId = buffer.read
  }

  /** Calculate the data packet size, excluding the header length.
    *
    * @return Header length
    */
  def calcPacketSize: Int = this.HEADER_SIZE

  /** abstract write packet body method, implement by sub packet protocol
    */
  def write2Buffer(buffer: MySQLPacketBuffer): Unit

  /** Return the binary stream of the packet
    *
    * @param charset we used to serialize
    * @return
    */
  def toByteBuffer(charset: String): ByteBuffer = {
    setCharset(charset)
    val size   = calcPacketSize
    val buffer = new MySQLPacketBuffer(size)
    buffer.init(charset)
    write2Buffer(buffer)
    afterPacketWritten(buffer)
    buffer.toByteBuffer
  }

  protected def afterPacketWritten(buffer: MySQLPacketBuffer): Unit = {
    val position = buffer.getPosition
    packetLength = position - HEADER_SIZE
    buffer.setPosition(0)
    buffer.write((packetLength & 0xff).toByte)
    buffer.write((packetLength >>> 8).toByte)
    buffer.write((packetLength >>> 16).toByte)
    buffer.write((packetId & 0xff).toByte)
    buffer.setPosition(position)
  }

  /** Abstract get package info */
  def getPacketInfo: String

  protected def setCharset(charset: String): Unit = {
    this.charset = CharsetUtil.getJavaCharsetFromMysql(charset)
  }
}
