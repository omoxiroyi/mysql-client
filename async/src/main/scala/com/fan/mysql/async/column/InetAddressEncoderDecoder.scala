

package com.fan.mysql.async.column

import java.net.InetAddress

import sun.net.util.IPAddressUtil.{textToNumericFormatV4, textToNumericFormatV6}

object InetAddressEncoderDecoder extends ColumnEncoderDecoder {

  override def decode(value: String): Any = {
    if (value contains ':') {
      InetAddress.getByAddress(textToNumericFormatV6(value))
    } else {
      InetAddress.getByAddress(textToNumericFormatV4(value))
    }
  }

  override def encode(value: Any): String = {
    value.asInstanceOf[InetAddress].getHostAddress
  }

}
