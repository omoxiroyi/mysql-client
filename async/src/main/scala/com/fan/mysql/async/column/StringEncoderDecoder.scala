

package com.fan.mysql.async.column

object StringEncoderDecoder extends ColumnEncoderDecoder {
  override def decode(value: String): String = value
}
