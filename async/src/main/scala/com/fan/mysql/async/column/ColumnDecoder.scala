package com.fan.mysql.async.column

import java.nio.charset.Charset

import com.fan.mysql.async.general.ColumnData
import io.netty.buffer.ByteBuf

trait ColumnDecoder {

  def decode(kind: ColumnData, value: ByteBuf, charset: Charset): Any = {
    val bytes = new Array[Byte](value.readableBytes())
    value.readBytes(bytes)
    decode(new String(bytes, charset))
  }

  def decode(value: String): Any

  def supportsStringDecoding: Boolean = true

}
