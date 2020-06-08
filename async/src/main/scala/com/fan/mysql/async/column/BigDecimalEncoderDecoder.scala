

package com.fan.mysql.async.column

object BigDecimalEncoderDecoder extends ColumnEncoderDecoder {

  override def decode(value: String): Any = BigDecimal(value)

}
