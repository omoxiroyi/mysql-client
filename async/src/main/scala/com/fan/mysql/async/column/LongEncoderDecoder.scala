

package com.fan.mysql.async.column

object LongEncoderDecoder extends ColumnEncoderDecoder {
  override def decode(value: String): Long = value.toLong
}