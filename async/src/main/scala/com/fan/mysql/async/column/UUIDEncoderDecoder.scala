

package com.fan.mysql.async.column

import java.util.UUID

object UUIDEncoderDecoder extends ColumnEncoderDecoder {

  override def decode(value: String): UUID = UUID.fromString(value)

}
