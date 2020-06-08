

package com.fan.mysql.async.binary.decoder

import io.netty.buffer.ByteBuf
import org.joda.time.LocalDate

object DateDecoder extends BinaryDecoder {
  override def decode(buffer: ByteBuf): LocalDate = {
    val result = TimestampDecoder.decode(buffer)

    if (result != null) {
      result.toLocalDate
    } else {
      null
    }
  }
}
