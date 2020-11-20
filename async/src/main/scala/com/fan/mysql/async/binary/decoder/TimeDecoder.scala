package com.fan.mysql.async.binary.decoder

import io.netty.buffer.ByteBuf

import scala.concurrent.duration._

object TimeDecoder extends BinaryDecoder {
  def decode(buffer: ByteBuf): Duration = {

    buffer.readUnsignedByte() match {
      case 0 => 0.seconds
      case 8 => {

        val isNegative = buffer.readUnsignedByte() == 1

        val duration = buffer.readUnsignedInt().days +
          buffer.readUnsignedByte().hours +
          buffer.readUnsignedByte().minutes +
          buffer.readUnsignedByte().seconds

        if (isNegative) {
          duration.neg()
        } else {
          duration
        }

      }
      case 12 => {

        val isNegative = buffer.readUnsignedByte() == 1

        val duration = buffer.readUnsignedInt().days +
          buffer.readUnsignedByte().hours +
          buffer.readUnsignedByte().minutes +
          buffer.readUnsignedByte().seconds +
          buffer.readUnsignedInt().micros

        if (isNegative) {
          duration.neg()
        } else {
          duration
        }

      }
    }

  }
}
