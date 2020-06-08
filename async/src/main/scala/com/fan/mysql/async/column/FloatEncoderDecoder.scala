

package com.fan.mysql.async.column

object FloatEncoderDecoder extends ColumnEncoderDecoder {
  override def decode(value: String): Float = value.toFloat
}
