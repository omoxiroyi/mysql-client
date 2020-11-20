package com.fan.mysql.async.column

import java.nio.charset.Charset

import com.fan.mysql.async.general.ColumnData
import io.netty.buffer.ByteBuf

trait ColumnDecoderRegistry {

  def decode(kind: ColumnData, value: ByteBuf, charset: Charset): Any

}
