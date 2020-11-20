package com.fan.mysql.async.encoder

import java.nio.charset.Charset

import com.fan.mysql.async.message.client.{ClientMessage, QueryMessage}
import com.fan.mysql.async.util.ByteBufferUtils
import io.netty.buffer.ByteBuf

class QueryMessageEncoder(charset: Charset) extends MessageEncoder {

  def encode(message: ClientMessage): ByteBuf = {

    val m = message.asInstanceOf[QueryMessage]
    val encodedQuery = m.query.getBytes(charset)

    val buffer = ByteBufferUtils.packetBuffer(4 + 1 + encodedQuery.length)

    buffer.writeByte(ClientMessage.Query)
    buffer.writeBytes(encodedQuery)

    buffer
  }

}
