package com.fan.mysql.async.column

object ByteDecoder extends ColumnDecoder {
  override def decode(value: String): Any = value.toByte
}
