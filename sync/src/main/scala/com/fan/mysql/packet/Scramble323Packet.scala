package com.fan.mysql.packet

import com.fan.mysql.util.MySQLPacketBuffer

/**
 * <pre>
 * By sending this very specific reply server asks us to send scrambled
 * password in old format. The reply contains scramble_323.
 * </pre>
 *
 * @author fan
 */
class Scramble323Packet extends MySQLPacket {

  var seed: String = _

  override def write2Buffer(buffer: MySQLPacketBuffer): Unit = {
    buffer.writeStringWithNull(this.seed)
  }

  override def calcPacketSize: Int = {
    var size = super.calcPacketSize
    size += (if (this.seed == null) 1
    else this.seed.length + 1)
    size
  }

  override def getPacketInfo = "MySQL Scramble323 Packet"

}
