

package com.fan.mysql.async.column

object IntegerEncoderDecoder extends ColumnEncoderDecoder {

  override def decode(value: String): Int = value.toInt

}
