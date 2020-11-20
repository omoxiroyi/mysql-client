package com.fan.mysql.async.encoder

import java.nio.charset.Charset

import com.fan.mysql.async.message.client.{ClientMessage, PreparedStatementPrepareMessage}
import com.fan.mysql.async.util.ByteBufferUtils
import io.netty.buffer.ByteBuf

class PreparedStatementPrepareEncoder(charset: Charset) extends MessageEncoder {

  def encode(message: ClientMessage): ByteBuf = {
    val m = message.asInstanceOf[PreparedStatementPrepareMessage]
    val statement = m.statement.getBytes(charset)
    val buffer = ByteBufferUtils.packetBuffer(4 + 1 + statement.size)
    buffer.writeByte(m.kind)
    buffer.writeBytes(statement)

    buffer
  }

}
