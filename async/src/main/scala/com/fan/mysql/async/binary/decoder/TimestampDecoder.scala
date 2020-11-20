package com.fan.mysql.async.binary.decoder

import io.netty.buffer.ByteBuf
import org.joda.time.LocalDateTime

object TimestampDecoder extends BinaryDecoder {
  def decode(buffer: ByteBuf): LocalDateTime = {
    val size = buffer.readUnsignedByte()

    size match {
      case 0 => null
      case 4 =>
        new LocalDateTime()
          .withDate(
            buffer.readUnsignedShort(),
            buffer.readUnsignedByte(),
            buffer.readUnsignedByte()
          )
          .withTime(0, 0, 0, 0)
      case 7 =>
        new LocalDateTime()
          .withDate(
            buffer.readUnsignedShort(),
            buffer.readUnsignedByte(),
            buffer.readUnsignedByte()
          )
          .withTime(
            buffer.readUnsignedByte(),
            buffer.readUnsignedByte(),
            buffer.readUnsignedByte(),
            0
          )
      case 11 =>
        new LocalDateTime()
          .withDate(
            buffer.readUnsignedShort(),
            buffer.readUnsignedByte(),
            buffer.readUnsignedByte()
          )
          .withTime(
            buffer.readUnsignedByte(),
            buffer.readUnsignedByte(),
            buffer.readUnsignedByte(),
            buffer.readUnsignedInt().toInt / 1000
          )
    }
  }
}
