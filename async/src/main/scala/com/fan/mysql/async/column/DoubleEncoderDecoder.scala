

package com.fan.mysql.async.column

object DoubleEncoderDecoder extends ColumnEncoderDecoder {
  override def decode(value: String): Double = value.toDouble
}
