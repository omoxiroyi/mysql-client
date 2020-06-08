

package com.fan.mysql.async.column

object ShortEncoderDecoder extends ColumnEncoderDecoder {

  override def decode(value: String): Any = value.toShort

}