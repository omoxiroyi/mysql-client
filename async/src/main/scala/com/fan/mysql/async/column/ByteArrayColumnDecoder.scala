

package com.fan.mysql.async.column

import java.nio.charset.Charset

import com.fan.mysql.async.general.ColumnData
import io.netty.buffer.ByteBuf


object ByteArrayColumnDecoder extends ColumnDecoder {

  override def decode(kind: ColumnData, value: ByteBuf, charset: Charset): Any = {
    val bytes = new Array[Byte](value.readableBytes())
    value.readBytes(bytes)
    bytes
  }

  def decode(value: String): Any = {
    throw new UnsupportedOperationException("This method should never be called for byte arrays")
  }
}
