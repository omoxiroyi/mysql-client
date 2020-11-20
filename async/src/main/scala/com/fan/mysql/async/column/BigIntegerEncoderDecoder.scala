package com.fan.mysql.async.column

object BigIntegerEncoderDecoder extends ColumnEncoderDecoder {
  override def decode(value: String): Any = BigInt(value)
}
