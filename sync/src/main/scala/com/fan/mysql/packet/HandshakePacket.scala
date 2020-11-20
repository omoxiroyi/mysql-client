package com.fan.mysql.packet

import com.fan.mysql.config.Capabilities
import com.fan.mysql.util.MySQLPacketBuffer

/** <pre>
  * 1              [0a] protocol version
  * string[NUL]    server version
  * 4              connection id
  * string[8]      auth-plugin-data-part-1
  * 1              [00] filler
  * 2              capability flags (lower 2 bytes)
  * if more data in the packet:
  * 1              character set
  * 2              status flags
  * 2              capability flags (upper 2 bytes)
  * if capabilities & CLIENT_PLUGIN_AUTH {
  * 1              length of auth-plugin-data
  * } else {
  * 1              [00]
  * }
  * string[10]     reserved (all [00])
  * if capabilities & CLIENT_SECURE_CONNECTION {
  * string[$len]   auth-plugin-data-part-2 ($len=MAX(13, length of auth-plugin-data - 8))
  * }
  * if capabilities & CLIENT_PLUGIN_AUTH {
  * string[NUL]    auth-plugin name
  * }
  * </pre>
  *
  * @author fan
  */
class HandshakePacket extends MySQLPacket {
  var protocolVersion: Byte = _
  var serverVersion: String = _
  var connectionId: Long    = _

  /** 8个字节 */
  var authPluginData1: String  = _
  var lowerCapabilities: Int   = _
  var serverCharsetIndex: Byte = _
  var serverStatus: Int        = _
  var upperCapabilities: Int   = _
  var authPluginDataLen: Int   = _
  var authPluginData2: String  = _
  var authPluginName: String   = _

  override def init(buffer: MySQLPacketBuffer, charset: String): Unit = {
    super.init(buffer, charset)
    this.protocolVersion = buffer.read
    this.serverVersion = buffer.readStringWithNull
    this.connectionId = buffer.readUB4
    this.authPluginData1 = buffer.readLengthString(8)
    buffer.move(1)
    this.lowerCapabilities = buffer.readUB2
    if (buffer.hasRemaining) {
      this.serverCharsetIndex = buffer.read
      this.serverStatus = buffer.readUB2
      this.upperCapabilities = buffer.readUB2
      val serverCapabilities = getServerCapabilities
      if ((serverCapabilities & Capabilities.CLIENT_PLUGIN_AUTH) > 0)
        this.authPluginDataLen = buffer.read
      else {
        this.authPluginDataLen = buffer.read
        this.authPluginDataLen = 0
      }
      buffer.move(10)
      if ((serverCapabilities & Capabilities.CLIENT_SECURE_CONNECTION) > 0)
        if (authPluginDataLen > 0) {
          val len = Math.max(13, authPluginDataLen - 8)
          this.authPluginData2 = buffer.readLengthString(12)
          buffer.move(len - 12)
        } else this.authPluginData2 = buffer.readLengthString(12)
      if ((serverCapabilities & Capabilities.CLIENT_PLUGIN_AUTH) > 0)
        this.authPluginName = buffer.readStringWithNull
    }
  }

  def getServerCapabilities: Int = this.lowerCapabilities | (this.upperCapabilities << 16)

  override def write2Buffer(buffer: MySQLPacketBuffer): Unit = {
    buffer.write(protocolVersion)
    buffer.writeStringWithNull(serverVersion)
    buffer.writeUB4(connectionId)
    buffer.writeStringNoNull(authPluginData1)
    buffer.write(0.toByte)
    buffer.writeUB2(lowerCapabilities)
    buffer.write(serverCharsetIndex)
    buffer.writeUB2(serverStatus)
    buffer.writeUB2(upperCapabilities)
    val serverCapabilities = getServerCapabilities
    if ((serverCapabilities & Capabilities.CLIENT_PLUGIN_AUTH) > 0)
      buffer.write(authPluginDataLen.toByte)
    else buffer.write(0.toByte)
    // reserved (all [00])
    buffer.writeBytesNoNull(new Array[Byte](10))
    if ((serverCapabilities & Capabilities.CLIENT_SECURE_CONNECTION) > 0) {
      val len = authPluginData2.getBytes.length
      if (len > 12) buffer.writeStringNoNull(authPluginData2)
      else buffer.writeStringWithNull(authPluginData2)
    }
    if ((serverCapabilities & Capabilities.CLIENT_PLUGIN_AUTH) > 0)
      buffer.writeStringWithNull(this.authPluginName)
  }

  override def calcPacketSize: Int = {
    var packetSize = super.calcPacketSize
    packetSize += 1 // protocolVersion

    packetSize += (if (this.serverVersion == null) 0
                   else this.serverVersion.getBytes.length) + 1
    packetSize += 4 // connectionId

    packetSize += 8 // authPluginData1

    packetSize += 1 // filler

    packetSize += 18 // 2+1+2+2+1+10

    val serverCapabilities: Int = getServerCapabilities
    if ((serverCapabilities & Capabilities.CLIENT_SECURE_CONNECTION) > 0) {
      val len = this.authPluginData2.getBytes.length
      if (len > 12) packetSize += len
      else packetSize += len + 1
    }
    if ((serverCapabilities & Capabilities.CLIENT_PLUGIN_AUTH) > 0) {
      packetSize += (if (this.authPluginName == null) 1
                     else this.authPluginName.getBytes.length)
    }
    packetSize
  }

  override def getPacketInfo = "MySQL Handshake Packet"

}
