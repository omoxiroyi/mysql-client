
package com.fan.mysql.async.decoder

import java.nio.charset.Charset

import com.fan.mysql.async.codec.DecoderRegistry
import com.fan.mysql.async.message.server.ColumnDefinitionMessage
import com.fan.mysql.async.util.ChannelWrapper._
import com.fan.mysql.async.util.Log
import io.netty.buffer.ByteBuf

object ColumnDefinitionDecoder {
  final val log = Log.get[ColumnDefinitionDecoder]
}

class ColumnDefinitionDecoder(charset: Charset, registry: DecoderRegistry) extends MessageDecoder {

  override def decode(buffer: ByteBuf): ColumnDefinitionMessage = {

    val catalog = buffer.readLengthEncodedString(charset)
    val schema = buffer.readLengthEncodedString(charset)
    val table = buffer.readLengthEncodedString(charset)
    val originalTable = buffer.readLengthEncodedString(charset)
    val name = buffer.readLengthEncodedString(charset)
    val originalName = buffer.readLengthEncodedString(charset)

    buffer.readBinaryLength

    val characterSet = buffer.readUnsignedShort()
    val columnLength = buffer.readUnsignedInt()
    val columnType = buffer.readUnsignedByte()
    val flags = buffer.readShort()
    val decimals = buffer.readByte()

    buffer.readShort()

    ColumnDefinitionMessage(
      catalog,
      schema,
      table,
      originalTable,
      name,
      originalName,
      characterSet,
      columnLength,
      columnType,
      flags,
      decimals,
      registry.binaryDecoderFor(columnType, characterSet),
      registry.textDecoderFor(columnType, characterSet)
    )
  }

}